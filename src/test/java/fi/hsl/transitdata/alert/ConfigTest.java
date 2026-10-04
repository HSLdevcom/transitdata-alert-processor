package fi.hsl.transitdata.alert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import org.junit.Test;

/**
 * Pins the defaults of the configuration that {@link Main} loads (environment.conf merged with transitdata-common's
 * common.conf), as seen when none of the override environment variables are set.
 */
public class ConfigTest {

    private final Config config = ConfigParser.createConfig();

    @Test
    public void pulsarTopicsAndSubscriptionDefaults() {
        assertEquals("omm-service-alert", config.getString("pulsar.consumer.topic"));
        assertEquals("transitdata-alert-processor-subscription", config.getString("pulsar.consumer.subscription"));
        assertEquals("gtfs-service-alert", config.getString("pulsar.producer.topic"));
    }

    @Test
    public void pulsarConnectionDefaults() {
        assertEquals("localhost", config.getString("pulsar.host"));
        assertEquals(6650, config.getInt("pulsar.port"));
        assertTrue(config.getBoolean("pulsar.consumer.enabled"));
        assertTrue(config.getBoolean("pulsar.producer.enabled"));
        assertFalse(config.getBoolean("pulsar.producer.multipleProducers"));
        assertEquals("Exclusive", config.getString("pulsar.consumer.subscriptionType"));
    }

    @Test
    public void globalNoServiceAlertsAreEnabledByDefault() {
        assertTrue(config.getBoolean("application.enableGlobalNoServiceAlerts"));
    }

    @Test
    public void optionalIntegrationsAreDisabledByDefault() {
        assertFalse(config.getBoolean("redis.enabled"));
        assertFalse(config.getBoolean("health.enabled"));
        assertFalse(config.getBoolean("pulsar.admin.enabled"));
    }
}
