package fi.hsl.transitdata.alert;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Functional tests of {@link AlertHandler#handleMessage} with the Pulsar consumer and producer mocked: consumed
 * message → GTFS-RT feed → produced message, and acknowledgement on every path.
 */
public class AlertHandlerMessageTest {

    private static final long EVENT_TIME_MS = 1_557_900_000_123L;

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private TypedMessageBuilder<byte[]> messageBuilder;
    private MessageId messageId;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        consumer = mock(Consumer.class);
        producer = mock(Producer.class);
        messageBuilder = mock(TypedMessageBuilder.class, RETURNS_SELF);
        messageId = mock(MessageId.class);

        when(producer.newMessage()).thenReturn(messageBuilder);
        when(consumer.acknowledgeAsync(any(MessageId.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    private AlertHandler handler(boolean enableGlobalNoServiceAlerts) {
        Config config = ConfigFactory
                .parseString("application.enableGlobalNoServiceAlerts=" + enableGlobalNoServiceAlerts);
        PulsarApplicationContext context = mock(PulsarApplicationContext.class);
        when(context.getConsumer()).thenReturn(consumer);
        when(context.getSingleProducer()).thenReturn(producer);
        when(context.getConfig()).thenReturn(config);
        return new AlertHandler(context);
    }

    @SuppressWarnings("unchecked")
    private Message<byte[]> message(byte[] data, String schema) {
        Message<byte[]> message = mock(Message.class);
        when(message.getData()).thenReturn(data);
        when(message.getEventTime()).thenReturn(EVENT_TIME_MS);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA)).thenReturn(schema);
        return message;
    }

    private static byte[] alertFixture() throws IOException {
        try (InputStream is = AlertHandlerMessageTest.class.getClassLoader().getResourceAsStream("alert.pb")) {
            return is.readAllBytes();
        }
    }

    private static byte[] serviceAlert(InternalMessages.Bulletin... bulletins) {
        InternalMessages.ServiceAlert.Builder builder = InternalMessages.ServiceAlert.newBuilder().setSchemaVersion(1);
        for (InternalMessages.Bulletin b : bulletins) {
            builder.addBulletins(b);
        }
        return builder.build().toByteArray();
    }

    private static InternalMessages.Bulletin cancelledEverywhere() {
        return InternalMessages.Bulletin.newBuilder().setBulletinId("strike")
                .setCategory(InternalMessages.Category.STRIKE).setImpact(InternalMessages.Bulletin.Impact.CANCELLED)
                .setPriority(InternalMessages.Bulletin.Priority.SEVERE).setLastModifiedUtcMs(0).setValidFromUtcMs(0)
                .setValidToUtcMs(1000).setAffectsAllRoutes(true).build();
    }

    private GtfsRealtime.FeedMessage capturedFeed() throws Exception {
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(messageBuilder).value(payload.capture());
        return GtfsRealtime.FeedMessage.parseFrom(payload.getValue());
    }

    @Test
    public void validServiceAlertIsConvertedToFullGtfsRtFeedAndProduced() throws Exception {
        byte[] data = alertFixture();
        InternalMessages.ServiceAlert input = InternalMessages.ServiceAlert.parseFrom(data);

        handler(true)
                .handleMessage(message(data, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString()));

        GtfsRealtime.FeedMessage feed = capturedFeed();
        assertEquals(EVENT_TIME_MS / 1000, feed.getHeader().getTimestamp());
        assertEquals(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET, feed.getHeader().getIncrementality());
        assertEquals(AlertHandler.createFeedEntities(input.getBulletinsList(), true), feed.getEntityList());
        verify(messageBuilder).eventTime(EVENT_TIME_MS);
        verify(messageBuilder).property(TransitdataProperties.KEY_PROTOBUF_SCHEMA,
                TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert.toString());
        verify(messageBuilder).send();
        verify(consumer).acknowledgeAsync(messageId);
    }

    @Test
    public void globalNoServiceAlertsSettingIsReadFromConfig() throws Exception {
        String schema = TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString();

        handler(false).handleMessage(message(serviceAlert(cancelledEverywhere()), schema));

        assertEquals(GtfsRealtime.Alert.Effect.REDUCED_SERVICE, capturedFeed().getEntity(0).getAlert().getEffect());
    }

    @Test
    public void globalNoServiceAlertsEnabledKeepsNoService() throws Exception {
        String schema = TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString();

        handler(true).handleMessage(message(serviceAlert(cancelledEverywhere()), schema));

        assertEquals(GtfsRealtime.Alert.Effect.NO_SERVICE, capturedFeed().getEntity(0).getAlert().getEffect());
    }

    @Test
    public void serviceAlertWithoutBulletinsProducesEmptyFeed() throws Exception {
        String schema = TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString();

        handler(true).handleMessage(message(serviceAlert(), schema));

        GtfsRealtime.FeedMessage feed = capturedFeed();
        assertEquals(0, feed.getEntityCount());
        assertEquals(EVENT_TIME_MS / 1000, feed.getHeader().getTimestamp());
        verify(consumer).acknowledgeAsync(messageId);
    }

    @Test
    public void messageWithWrongSchemaIsAcknowledgedButNotProduced() throws Exception {
        handler(true).handleMessage(
                message(alertFixture(), TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert.toString()));

        verify(producer, never()).newMessage();
        verify(consumer).acknowledgeAsync(messageId);
    }

    @Test
    public void messageWithoutSchemaIsAcknowledgedButNotProduced() throws Exception {
        handler(true).handleMessage(message(alertFixture(), null));

        verify(producer, never()).newMessage();
        verify(consumer).acknowledgeAsync(messageId);
    }

    @Test
    public void malformedPayloadIsAcknowledgedButNotProduced() {
        byte[] garbage = {(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01};

        handler(true).handleMessage(
                message(garbage, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString()));

        verify(producer, never()).newMessage();
        verify(consumer).acknowledgeAsync(messageId);
    }

    @Test
    public void failedSendIsSwallowedAndMessageIsStillAcknowledged() throws Exception {
        // Current behavior: a Pulsar send failure is logged and the input message is acked anyway, so the alert is lost
        when(messageBuilder.send()).thenThrow(new PulsarClientException("broker down"));

        handler(true).handleMessage(
                message(alertFixture(), TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString()));

        verify(messageBuilder).send();
        verify(consumer).acknowledgeAsync(messageId);
    }

    @Test
    public void unexpectedSendFailureIsSwallowedAndMessageIsStillAcknowledged() throws Exception {
        when(messageBuilder.send()).thenThrow(new IllegalStateException("unexpected"));

        handler(true).handleMessage(
                message(alertFixture(), TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString()));

        verify(consumer).acknowledgeAsync(messageId);
    }

    @Test
    public void failedAcknowledgementDoesNotThrow() throws Exception {
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new PulsarClientException("ack failed"));
        when(consumer.acknowledgeAsync(any(MessageId.class))).thenReturn(failed);

        handler(true).handleMessage(
                message(alertFixture(), TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString()));

        verify(messageBuilder).send();
    }

    @Test
    public void eventTimeZeroIsPassedToProducerAsIs() throws Exception {
        // Messages without an event time report 0. The handler forwards it unchanged; the real Pulsar client rejects
        // eventTime(0), so such alerts are dropped (see AlertProcessorIT#messageWithoutEventTimeIsDroppedAndAcked).
        Message<byte[]> message = message(serviceAlert(),
                TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString());
        when(message.getEventTime()).thenReturn(0L);

        handler(true).handleMessage(message);

        assertEquals(0L, capturedFeed().getHeader().getTimestamp());
        verify(messageBuilder).eventTime(0L);
        verify(messageBuilder, never()).key(anyString());
        verify(messageBuilder, never()).deliverAfter(anyLong(), any());
    }
}
