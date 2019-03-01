package fi.hsl.transitdata.omm;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.transitdata.omm.db.MockOmmConnector;
import fi.hsl.transitdata.omm.models.AlertState;
import fi.hsl.transitdata.omm.models.Bulletin;
import fi.hsl.transitdata.omm.models.Line;
import fi.hsl.transitdata.omm.models.StopPoint;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class OmmAlertHandlerTest {
    final static String TIMEZONE = "Europe/Helsinki";

    private MockOmmConnector readDefaultMockData() throws Exception {
        return MockOmmConnector.newInstance("2018_11_alert_dump.tsv");
    }

    private List<GtfsRealtime.FeedEntity> createFeedEntitiesFromDefaultMockData() throws Exception {
        MockOmmConnector connector = readDefaultMockData();
        List<Bulletin> bulletins = connector.getBulletinDAO().getActiveBulletins();
        Map<Long, Line> lines = connector.getLineDAO().getAllLines();
        Map<Long, StopPoint> stops = connector.getStopPointDAO().getAllStopPoints();

        List<GtfsRealtime.FeedEntity> feedEntities = OmmAlertHandler.createFeedEntities(bulletins, lines, stops, TIMEZONE);
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
        //last_modified	valid_from	valid_to	affects_all_routes	affects_all_stops	affected_route_ids	affected_stop_ids	title_fi	text_fi	title_sv	text_sv	title_en	text_en
        //2018-11-06 12:06:17	2018-11-06 12:04:00	2018-11-14 23:30:00	FALSE	FALSE	9011301022600000		Finnoonlahden pysäkki Hylkeenpyytäjäntiellä siirty	Finnoonlahden pysäkki Hylkeenpyytäjäntiellä siirtyy perjantaina 9.11. // HSL.fi	Inga trafikstörningar	Hållplats Finnoviken på Säljägarvägen flyttas på fredag 9.11. // HSL.fi	No traffic disruption	"Finnoonlahti" bus stop on Hylkeenpyytäjäntie relocated on Friday 9 November // HSL.fi
        assertTrue(entity.hasAlert());
        assertFalse(entity.hasTripUpdate());
        assertFalse(entity.hasVehicle());
        assertFalse(entity.hasIsDeleted());

        assertEquals("3593", entity.getId());
        GtfsRealtime.Alert alert = entity.getAlert();
        assertNotNull(alert);
        assertEquals(GtfsRealtime.Alert.Effect.NO_SERVICE, alert.getEffect());
        assertEquals(GtfsRealtime.Alert.Cause.OTHER_CAUSE, alert.getCause());
        assertEquals(1, alert.getActivePeriodCount());
        assertEquals(1, alert.getInformedEntityList().size());

        GtfsRealtime.EntitySelector selector = alert.getInformedEntity(0);
        assertFalse(selector.hasTrip());
        assertFalse(selector.hasStopId());
        assertFalse(selector.hasAgencyId());
        assertTrue(selector.hasRouteId());
        assertEquals(MockOmmConnector.lineGidToLineId(9011301022600000L), selector.getRouteId());

        GtfsRealtime.TranslatedString header = alert.getHeaderText();
        assertEquals(3, header.getTranslationCount());
    }

    @Test
    public void testTimestampConversion() throws Exception {
        MockOmmConnector connector = readDefaultMockData();
        List<Bulletin> bulletins = connector.getBulletinDAO().getActiveBulletins();
        AlertState state = new AlertState(bulletins);
        long utcMs = OmmAlertHandler.lastModifiedInUtcMs(state, TIMEZONE);
        assertEquals(1542621762000L, utcMs);
    }

    @Test
    public void testOneFeedEntityThoroughly() throws Exception {
        MockOmmConnector connector = readDefaultMockData();
        List<Bulletin> bulletins = connector.getBulletinDAO().getActiveBulletins();
        Map<Long, Line> lines = connector.getLineDAO().getAllLines();
        Map<Long, StopPoint> stops = connector.getStopPointDAO().getAllStopPoints();

        List<GtfsRealtime.FeedEntity> feedEntities = OmmAlertHandler.createFeedEntities(bulletins, lines, stops, TIMEZONE);
        Optional<GtfsRealtime.FeedEntity> maybeEntity = feedEntities.stream().filter(entity -> entity.getId().equals("3598")).findFirst();
        assertTrue(maybeEntity.isPresent());

        GtfsRealtime.Alert alert = maybeEntity.get().getAlert();

        assertEquals(Bulletin.Category.ROAD_MAINTENANCE.toGtfsCause(), alert.getCause());
        assertEquals(Bulletin.Impact.DELAYED.toGtfsEffect(), alert.getEffect());
        assertEquals(1, alert.getActivePeriodCount());
        assertEquals( 1541576820L, alert.getActivePeriod(0).getStart());
        assertEquals( 1542231000L, alert.getActivePeriod(0).getEnd());
        assertEquals(2, alert.getInformedEntityCount());

        List<GtfsRealtime.EntitySelector> entities = alert.getInformedEntityList();
        validateEntitySelector(entities.get(0),  MockOmmConnector.lineGidToLineId(9011301022700000L));
        validateEntitySelector(entities.get(1),  MockOmmConnector.lineGidToLineId(9011301095000000L));

        GtfsRealtime.TranslatedString description = alert.getDescriptionText();
        assertEquals(3, description.getTranslationCount());
        description.getTranslationList().forEach(translation -> {
            switch (translation.getLanguage()) {
                case "fi": assertEquals("Linjalla 112/N pysäkki Pattistenpelto (E2348) tilapäisesti poissa käytöstä 12.-21.11. / hsl.fi", translation.getText());
                    break;
                case "sv": assertEquals("Hållplats Battisåkern (E2348) på linje 112/N tillfälligt ur bruk 12.-21.11 / hsl.fi/sv", translation.getText());
                    break;
                case "en": assertEquals("-", translation.getText());
                    break;
                default: assertTrue(false);
            }
        });

        GtfsRealtime.TranslatedString header = alert.getHeaderText();
        assertEquals(3, header.getTranslationCount());
        header.getTranslationList().forEach(translation -> {
            switch (translation.getLanguage()) {
                case "fi": assertEquals("Pysäkki Pattistenpelto väliaikaisesti poissa", translation.getText());
                    break;
                case "sv": assertEquals("Hållplats Battisåkern tillfälligt ur bruk", translation.getText());
                    break;
                case "en": assertEquals("Pattistenpelto bus stop temporarily closed", translation.getText());
                    break;
                default: assertTrue(false);
            }
        });

    }

    private void validateEntitySelector(GtfsRealtime.EntitySelector entity, String id) {
        assertFalse(entity.hasAgencyId());
        assertFalse(entity.hasStopId());
        assertTrue(entity.hasRouteId());
        assertFalse(entity.hasRouteType());
        assertFalse(entity.hasTrip());
        assertEquals(id, entity.getRouteId());
    }


}
