package fi.hsl.transitdata.alert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end test against a real Pulsar broker: the application is wired exactly like {@link Main} (config from
 * environment.conf, {@link PulsarApplication}, {@link AlertHandler}) with only the broker address and topic names
 * overridden.
 */
public class AlertProcessorIT {

    // Same broker line as the pulsar-client version used by transitdata-common
    private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:3.0.7");
    private static final long EVENT_TIME_MS = 1_557_900_000_456L;

    private static PulsarContainer pulsar;
    private static PulsarClient client;

    private String inputTopic;
    private String outputTopic;
    private PulsarApplication app;
    private Thread appThread;
    private Producer<byte[]> inputProducer;
    private Consumer<byte[]> outputConsumer;

    @BeforeClass
    public static void startPulsar() throws Exception {
        pulsar = new PulsarContainer(PULSAR_IMAGE);
        pulsar.start();
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
    }

    @AfterClass
    public static void stopPulsar() throws Exception {
        if (client != null) {
            client.close();
        }
        if (pulsar != null) {
            pulsar.stop();
        }
    }

    @Before
    public void startApplication() throws Exception {
        String suffix = UUID.randomUUID().toString();
        inputTopic = "persistent://public/default/omm-service-alert-" + suffix;
        outputTopic = "persistent://public/default/gtfs-service-alert-" + suffix;

        outputConsumer = client.newConsumer().topic(outputTopic).subscriptionName("it-verifier")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        URI broker = URI.create(pulsar.getPulsarBrokerUrl());
        Config overrides = ConfigFactory.parseMap(Map.of("pulsar.host", broker.getHost(), "pulsar.port",
                broker.getPort(), "pulsar.consumer.topic", inputTopic, "pulsar.producer.topic", outputTopic));
        Config config = overrides.withFallback(ConfigParser.createConfig()).resolve();

        app = PulsarApplication.newInstance(config);
        AlertHandler handler = new AlertHandler(app.getContext());
        appThread = new Thread(() -> {
            try {
                app.launchWithHandler(handler);
            } catch (Exception e) {
                // launchWithHandler ends with an exception when the application is closed in tearDown
            }
        }, "alert-processor-it");
        appThread.setDaemon(true);
        appThread.start();

        inputProducer = client.newProducer().topic(inputTopic).create();
    }

    @After
    public void stopApplication() throws Exception {
        if (inputProducer != null) {
            inputProducer.close();
        }
        if (outputConsumer != null) {
            outputConsumer.close();
        }
        if (app != null) {
            app.close();
        }
        if (appThread != null) {
            appThread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    private static byte[] alertFixture() throws Exception {
        try (InputStream is = AlertProcessorIT.class.getClassLoader().getResourceAsStream("alert.pb")) {
            return is.readAllBytes();
        }
    }

    private void send(byte[] payload, TransitdataProperties.ProtobufSchema schema) throws Exception {
        inputProducer.newMessage().value(payload).eventTime(EVENT_TIME_MS)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString()).send();
    }

    @Test
    public void serviceAlertIsPublishedAsGtfsRtFeed() throws Exception {
        byte[] payload = alertFixture();
        InternalMessages.ServiceAlert input = InternalMessages.ServiceAlert.parseFrom(payload);

        send(payload, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert);

        Message<byte[]> output = outputConsumer.receive(30, TimeUnit.SECONDS);
        assertNotNull("no GTFS-RT message was produced", output);
        assertEquals(TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert.toString(),
                output.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA));
        assertEquals(EVENT_TIME_MS, output.getEventTime());

        GtfsRealtime.FeedMessage feed = GtfsRealtime.FeedMessage.parseFrom(output.getData());
        assertEquals(EVENT_TIME_MS / 1000, feed.getHeader().getTimestamp());
        assertEquals(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET, feed.getHeader().getIncrementality());
        assertEquals(input.getBulletinsCount(), feed.getEntityCount());
        assertEquals(AlertHandler.createFeedEntities(input.getBulletinsList(), true), feed.getEntityList());
    }

    @Test
    public void messagesWithWrongSchemaAreSkippedAndProcessingContinues() throws Exception {
        byte[] payload = alertFixture();

        send(payload, TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert);
        send(new byte[]{(byte) 0xff, 0x01}, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert);
        send(payload, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert);

        Message<byte[]> output = outputConsumer.receive(30, TimeUnit.SECONDS);
        assertNotNull("valid message after invalid ones was not processed", output);
        assertEquals(EVENT_TIME_MS / 1000,
                GtfsRealtime.FeedMessage.parseFrom(output.getData()).getHeader().getTimestamp());
        assertNull("invalid messages must not produce output", outputConsumer.receive(3, TimeUnit.SECONDS));
    }
}
