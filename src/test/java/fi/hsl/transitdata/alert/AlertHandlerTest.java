package fi.hsl.transitdata.alert;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;

public class AlertHandlerTest {

    private byte[] readProtobufFromResourceFile(final String filename) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(filename)) {
            return is.readAllBytes();
        }
    }

    private InternalMessages.ServiceAlert readDefaultMockData() throws IOException {
        final byte[] data = readProtobufFromResourceFile("alert.pb");
        InternalMessages.ServiceAlert alert = InternalMessages.ServiceAlert.parseFrom(data);
        return alert;
    }

    private List<GtfsRealtime.FeedEntity> createFeedEntitiesFromDefaultMockData() throws Exception {
        final InternalMessages.ServiceAlert alert = readDefaultMockData();
        final List<InternalMessages.Bulletin> bulletins = alert.getBulletinsList();

        List<GtfsRealtime.FeedEntity> feedEntities = AlertHandler.createFeedEntities(bulletins, true);
        assertEquals(bulletins.size(), feedEntities.size());
        validateMockDataFirstEntity(feedEntities.get(0));
        return feedEntities;
    }

    @Test
    public void testCreateFeedEntities() throws Exception {
        List<GtfsRealtime.FeedEntity> feedEntities = createFeedEntitiesFromDefaultMockData();

        feedEntities.forEach(entity -> {
            assertTrue(entity.hasAlert());
            assertTrue(entity.hasId());

            assertFalse(entity.hasTripUpdate());
            assertFalse(entity.hasVehicle());

            GtfsRealtime.Alert alert = entity.getAlert();
            assertTrue(alert.hasCause());
            assertTrue(alert.hasEffect());
            assertTrue(alert.hasDescriptionText());
            assertTrue(alert.hasHeaderText());
            assertTrue(alert.hasSeverityLevel());

        });
    }

    @Test
    public void testCreateFeedMessage() throws Exception {
        List<GtfsRealtime.FeedEntity> feedEntities = createFeedEntitiesFromDefaultMockData();
        final long timestamp = System.currentTimeMillis() / 1000;

        GtfsRealtime.FeedMessage msg = FeedMessageFactory.createFullFeedMessage(feedEntities, timestamp);

        assertNotNull(msg);
        assertEquals(timestamp, msg.getHeader().getTimestamp());
        assertEquals(feedEntities.size(), msg.getEntityCount());

        validateMockDataFirstEntity(msg.getEntity(0));
    }

    private void validateMockDataFirstEntity(GtfsRealtime.FeedEntity entity) {
        assertTrue(entity.hasAlert());
        assertFalse(entity.hasTripUpdate());
        assertFalse(entity.hasVehicle());
        assertFalse(entity.hasIsDeleted());

        assertEquals("6340", entity.getId());
        GtfsRealtime.Alert alert = entity.getAlert();
        assertNotNull(alert);
        assertEquals(GtfsRealtime.Alert.Effect.MODIFIED_SERVICE, alert.getEffect());
        assertEquals(GtfsRealtime.Alert.Cause.OTHER_CAUSE, alert.getCause());
        assertEquals(1, alert.getActivePeriodCount());
        assertEquals(3, alert.getInformedEntityList().size());
        assertEquals(GtfsRealtime.Alert.SeverityLevel.INFO, alert.getSeverityLevel());

        Collection<GtfsRealtime.EntitySelector> selectors = alert.getInformedEntityList();
        selectors.forEach(selector -> {
            assertFalse(selector.hasTrip());
            assertFalse(selector.hasStopId());
            assertFalse(selector.hasAgencyId());
            assertTrue(selector.hasRouteId());
        });
        assertThat(selectors.stream().map(GtfsRealtime.EntitySelector::getRouteId).collect(Collectors.toList()), hasItem("4562"));

        GtfsRealtime.TranslatedString header = alert.getHeaderText();
        assertEquals(3, header.getTranslationCount());

        GtfsRealtime.TranslatedString description = alert.getDescriptionText();
        assertEquals(3, description.getTranslationCount());

        GtfsRealtime.TranslatedString url = alert.getUrl();
        assertEquals(3, url.getTranslationCount());
    }

    @Test
    public void testOneFeedEntityThoroughly() throws Exception {
        final InternalMessages.ServiceAlert alert = readDefaultMockData();
        final List<InternalMessages.Bulletin> bulletins = alert.getBulletinsList();

        List<GtfsRealtime.FeedEntity> feedEntities = AlertHandler.createFeedEntities(bulletins, true);
        Optional<GtfsRealtime.FeedEntity> maybeEntity = feedEntities.stream().filter(entity -> entity.getId().equals("6431")).findFirst();
        assertTrue(maybeEntity.isPresent());

        GtfsRealtime.Alert gtfsAlert = maybeEntity.get().getAlert();

        assertEquals(AlertHandler.toGtfsCause(InternalMessages.Category.ROAD_CLOSED), gtfsAlert.getCause());
        assertEquals(AlertHandler.toGtfsEffect(InternalMessages.Bulletin.Impact.DISRUPTION_ROUTE), gtfsAlert.getEffect());
        assertEquals(1, gtfsAlert.getActivePeriodCount());
        assertEquals( 1557885600L, gtfsAlert.getActivePeriod(0).getStart());
        assertEquals( 1558198800L, gtfsAlert.getActivePeriod(0).getEnd());
        assertEquals(5, gtfsAlert.getInformedEntityCount());
        assertEquals(AlertHandler.toGtfsSeverityLevel(InternalMessages.Bulletin.Priority.INFO).get(), gtfsAlert.getSeverityLevel());

        List<GtfsRealtime.EntitySelector> entities = gtfsAlert.getInformedEntityList();
        validateEntitySelectors(entities, "1230103", "1230104", "1230101", "1232102", "1232104");

        GtfsRealtime.TranslatedString header = gtfsAlert.getHeaderText();
        assertEquals(3, header.getTranslationCount());
        header.getTranslationList().forEach(translation -> {
            switch (translation.getLanguage()) {
                case "fi": assertEquals("Hämeentie suljettu 18.5. Arabian katufestivaalin ", translation.getText());
                    break;
                case "sv": assertEquals("Gatan avstängd", translation.getText());
                    break;
                case "en": assertEquals("Road closed", translation.getText());
                    break;
                default: assertTrue(false);
            }
        });

        GtfsRealtime.TranslatedString description = gtfsAlert.getDescriptionText();
        assertEquals(3, description.getTranslationCount());
        description.getTranslationList().forEach(translation -> {
            switch (translation.getLanguage()) {
                case "fi": assertEquals("Linjat 52, 55, 71, 78N ja 506 Arabiassa poikkeusreiteillä la 18.5. klo 9-20. /Info: hsl.fi.", translation.getText());
                    break;
                case "sv": assertEquals("Linjerna 52, 55, 71, 78N och 506 kör avvikande rutter i Arabia 18.5 kl. 9-20. /Info: hsl.fi/sv", translation.getText());
                    break;
                case "en": assertEquals("Buses 52, 55, 71, 78N and 506 diverted in Arabia on 18 May 9am-8pm. /Info: hsl.fi/en", translation.getText());
                    break;
                default: assertTrue(false);
            }
        });

        GtfsRealtime.TranslatedString url = gtfsAlert.getUrl();
        assertEquals(3, url.getTranslationCount());
        url.getTranslationList().forEach(translation -> {
            switch (translation.getLanguage()) {
                case "fi": assertEquals("https://www.hsl.fi/", translation.getText());
                    break;
                case "sv": assertEquals("https://www.hsl.fi/sv", translation.getText());
                    break;
                case "en": assertEquals("https://www.hsl.fi/en", translation.getText());
                    break;
                default: assertTrue(false);
            }
        });
    }

    private void validateEntitySelectors(Collection<GtfsRealtime.EntitySelector> entities, String... ids) {
        for (GtfsRealtime.EntitySelector entity : entities) {
            assertFalse(entity.hasAgencyId());
            assertTrue(entity.hasStopId());
            assertFalse(entity.hasRouteId());
            assertFalse(entity.hasRouteType());
            assertFalse(entity.hasTrip());
            assertThat(Arrays.asList(ids), hasItem(entity.getStopId()));
        }
    }

    @Test
    public void testNoAlertIsCreatedForDisplayOnlyBulletin() {
        InternalMessages.Bulletin bulletin = InternalMessages.Bulletin.newBuilder()
                .setBulletinId("1")
                .setAffectsAllRoutes(true)
                .setAffectsAllStops(true)
                .setCategory(InternalMessages.Category.STRIKE)
                .setImpact(InternalMessages.Bulletin.Impact.CANCELLED)
                .setValidFromUtcMs(0)
                .setValidToUtcMs(Long.MAX_VALUE)
                .setLastModifiedUtcMs(Instant.now().getEpochSecond())
                .addDescriptions(InternalMessages.Bulletin.Translation.newBuilder().setLanguage("en").setText("Test"))
                .setDisplayOnly(true)
                .build();

        Optional<GtfsRealtime.Alert> alert = AlertHandler.createAlert(bulletin, true);
        assertFalse(alert.isPresent());
    }

    @Test
    public void testEntitySelectorRouteIdsAreNormalized() {
        List<InternalMessages.Bulletin.AffectedEntity> entities = Stream.of("1009", "1009 1", "1009 6")
                .map(routeId -> InternalMessages.Bulletin.AffectedEntity.newBuilder().setEntityId(routeId).build())
                .collect(Collectors.toList());
        InternalMessages.Bulletin bulletin = InternalMessages.Bulletin.newBuilder()
                .addAllAffectedRoutes(entities)
                .setLastModifiedUtcMs(System.currentTimeMillis())
                .setValidFromUtcMs(System.currentTimeMillis())
                .setValidToUtcMs(System.currentTimeMillis())
                .build();

        Collection<GtfsRealtime.EntitySelector> selectors = AlertHandler.entitySelectorsForBulletin(bulletin);
        assertEquals(1, selectors.size());
        assertTrue(selectors.contains(GtfsRealtime.EntitySelector.newBuilder().setRouteId("1009").build()));
        assertFalse(selectors.contains(GtfsRealtime.EntitySelector.newBuilder().setRouteId("1009 1").build()));
    }

    @Test
    public void testNoServiceEffectIsNotUsedWhenBulletinAffectAll() {
        InternalMessages.Bulletin bulletin = InternalMessages.Bulletin.newBuilder()
                .setBulletinId("1")
                .setAffectsAllRoutes(true)
                .setAffectsAllStops(true)
                .setCategory(InternalMessages.Category.STRIKE)
                .setImpact(InternalMessages.Bulletin.Impact.CANCELLED)
                .setValidFromUtcMs(0)
                .setValidToUtcMs(Long.MAX_VALUE)
                .setLastModifiedUtcMs(Instant.now().getEpochSecond())
                .addDescriptions(InternalMessages.Bulletin.Translation.newBuilder().setLanguage("en").setText("Test"))
                .setDisplayOnly(false)
                .build();

        Optional<GtfsRealtime.Alert> alert = AlertHandler.createAlert(bulletin, false);
        assertTrue(alert.isPresent());
        assertNotEquals(GtfsRealtime.Alert.Effect.NO_SERVICE, alert.get().getEffect());
    }
}
