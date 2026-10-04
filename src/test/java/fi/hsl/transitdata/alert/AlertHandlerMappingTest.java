package fi.hsl.transitdata.alert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.InternalMessages.Bulletin;
import fi.hsl.common.transitdata.proto.InternalMessages.Category;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

/**
 * Characterization tests pinning the current mapping from internal service alert bulletins to GTFS-RT alerts.
 */
public class AlertHandlerMappingTest {

    private static final Map<Category, GtfsRealtime.Alert.Cause> EXPECTED_CAUSES = new EnumMap<>(Category.class);
    private static final Map<Bulletin.Impact, GtfsRealtime.Alert.Effect> EXPECTED_EFFECTS = new EnumMap<>(
            Bulletin.Impact.class);

    static {
        for (Category c : new Category[]{Category.OTHER_DRIVER_ERROR, Category.TOO_MANY_PASSENGERS,
                Category.MISPARKED_VEHICLE, Category.TEST, Category.STATE_VISIT, Category.TRACK_BLOCKED,
                Category.EARLIER_DISRUPTION, Category.OTHER, Category.NO_TRAFFIC_DISRUPTION, Category.TRAFFIC_JAM,
                Category.PUBLIC_EVENT, Category.STAFF_DEFICIT, Category.DISTURBANCE}) {
            EXPECTED_CAUSES.put(c, GtfsRealtime.Alert.Cause.OTHER_CAUSE);
        }
        for (Category c : new Category[]{Category.ITS_SYSTEM_ERROR, Category.SWITCH_FAILURE, Category.TECHNICAL_FAILURE,
                Category.VEHICLE_BREAKDOWN, Category.POWER_FAILURE, Category.VEHICLE_DEFICIT}) {
            EXPECTED_CAUSES.put(c, GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM);
        }
        EXPECTED_CAUSES.put(Category.STRIKE, GtfsRealtime.Alert.Cause.STRIKE);
        EXPECTED_CAUSES.put(Category.VEHICLE_OFF_THE_ROAD, GtfsRealtime.Alert.Cause.ACCIDENT);
        EXPECTED_CAUSES.put(Category.TRAFFIC_ACCIDENT, GtfsRealtime.Alert.Cause.ACCIDENT);
        EXPECTED_CAUSES.put(Category.ACCIDENT, GtfsRealtime.Alert.Cause.ACCIDENT);
        EXPECTED_CAUSES.put(Category.SEIZURE, GtfsRealtime.Alert.Cause.MEDICAL_EMERGENCY);
        EXPECTED_CAUSES.put(Category.MEDICAL_INCIDENT, GtfsRealtime.Alert.Cause.MEDICAL_EMERGENCY);
        EXPECTED_CAUSES.put(Category.WEATHER, GtfsRealtime.Alert.Cause.WEATHER);
        EXPECTED_CAUSES.put(Category.WEATHER_CONDITIONS, GtfsRealtime.Alert.Cause.WEATHER);
        EXPECTED_CAUSES.put(Category.ROAD_MAINTENANCE, GtfsRealtime.Alert.Cause.MAINTENANCE);
        EXPECTED_CAUSES.put(Category.TRACK_MAINTENANCE, GtfsRealtime.Alert.Cause.MAINTENANCE);
        EXPECTED_CAUSES.put(Category.ROAD_CLOSED, GtfsRealtime.Alert.Cause.CONSTRUCTION);
        EXPECTED_CAUSES.put(Category.ROAD_TRENCH, GtfsRealtime.Alert.Cause.CONSTRUCTION);
        EXPECTED_CAUSES.put(Category.ASSAULT, GtfsRealtime.Alert.Cause.POLICE_ACTIVITY);
        // Not mapped explicitly, fall through to the default branch
        EXPECTED_CAUSES.put(Category.NO_DRIVER, GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE);
        EXPECTED_CAUSES.put(Category.CHARGING_SERVICE, GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE);

        EXPECTED_EFFECTS.put(Bulletin.Impact.CANCELLED, GtfsRealtime.Alert.Effect.NO_SERVICE);
        EXPECTED_EFFECTS.put(Bulletin.Impact.DELAYED, GtfsRealtime.Alert.Effect.SIGNIFICANT_DELAYS);
        EXPECTED_EFFECTS.put(Bulletin.Impact.IRREGULAR_DEPARTURES, GtfsRealtime.Alert.Effect.SIGNIFICANT_DELAYS);
        EXPECTED_EFFECTS.put(Bulletin.Impact.DEVIATING_SCHEDULE, GtfsRealtime.Alert.Effect.MODIFIED_SERVICE);
        EXPECTED_EFFECTS.put(Bulletin.Impact.POSSIBLE_DEVIATIONS, GtfsRealtime.Alert.Effect.MODIFIED_SERVICE);
        EXPECTED_EFFECTS.put(Bulletin.Impact.DISRUPTION_ROUTE, GtfsRealtime.Alert.Effect.DETOUR);
        EXPECTED_EFFECTS.put(Bulletin.Impact.POSSIBLY_DELAYED, GtfsRealtime.Alert.Effect.OTHER_EFFECT);
        EXPECTED_EFFECTS.put(Bulletin.Impact.VENDING_MACHINE_OUT_OF_ORDER, GtfsRealtime.Alert.Effect.OTHER_EFFECT);
        EXPECTED_EFFECTS.put(Bulletin.Impact.RETURNING_TO_NORMAL, GtfsRealtime.Alert.Effect.OTHER_EFFECT);
        EXPECTED_EFFECTS.put(Bulletin.Impact.OTHER, GtfsRealtime.Alert.Effect.OTHER_EFFECT);
        EXPECTED_EFFECTS.put(Bulletin.Impact.REDUCED_TRANSPORT, GtfsRealtime.Alert.Effect.REDUCED_SERVICE);
        EXPECTED_EFFECTS.put(Bulletin.Impact.NO_TRAFFIC_IMPACT, GtfsRealtime.Alert.Effect.NO_EFFECT);
        // Not mapped explicitly, fall through to the default branch
        EXPECTED_EFFECTS.put(Bulletin.Impact.NULL, GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT);
        EXPECTED_EFFECTS.put(Bulletin.Impact.UNKNOWN, GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT);
    }

    private static Bulletin.Translation translation(String language, String text) {
        return Bulletin.Translation.newBuilder().setLanguage(language).setText(text).build();
    }

    private static Bulletin.AffectedEntity entity(String id) {
        return Bulletin.AffectedEntity.newBuilder().setEntityId(id).build();
    }

    private static Bulletin.Builder bulletin() {
        return Bulletin.newBuilder().setBulletinId("b1").setCategory(Category.ROAD_CLOSED)
                .setImpact(Bulletin.Impact.DISRUPTION_ROUTE).setPriority(Bulletin.Priority.WARNING)
                .setLastModifiedUtcMs(1_700_000_000_000L).setValidFromUtcMs(1_700_000_000_999L)
                .setValidToUtcMs(1_700_003_600_500L).addAffectedStops(entity("1230103"))
                .addTitles(translation("fi", "Otsikko")).addTitles(translation("en", "Title"))
                .addDescriptions(translation("fi", "Kuvaus")).addUrls(translation("fi", "https://www.hsl.fi/"));
    }

    @Test
    public void everyCategoryMapsToTheExpectedCause() {
        assertEquals("every category value must be covered", Category.values().length, EXPECTED_CAUSES.size());
        for (Category category : Category.values()) {
            assertEquals(category.name(), EXPECTED_CAUSES.get(category), AlertHandler.toGtfsCause(category));
        }
    }

    @Test
    public void everyImpactMapsToTheExpectedEffect() {
        assertEquals("every impact value must be covered", Bulletin.Impact.values().length, EXPECTED_EFFECTS.size());
        for (Bulletin.Impact impact : Bulletin.Impact.values()) {
            assertEquals(impact.name(), EXPECTED_EFFECTS.get(impact), AlertHandler.toGtfsEffect(impact));
        }
    }

    @Test
    public void everyPriorityMapsToASeverityLevel() {
        assertEquals(Optional.of(GtfsRealtime.Alert.SeverityLevel.INFO),
                AlertHandler.toGtfsSeverityLevel(Bulletin.Priority.INFO));
        assertEquals(Optional.of(GtfsRealtime.Alert.SeverityLevel.WARNING),
                AlertHandler.toGtfsSeverityLevel(Bulletin.Priority.WARNING));
        assertEquals(Optional.of(GtfsRealtime.Alert.SeverityLevel.SEVERE),
                AlertHandler.toGtfsSeverityLevel(Bulletin.Priority.SEVERE));
        assertEquals(3, Bulletin.Priority.values().length);
    }

    @Test
    public void translatedStringKeepsOrderLanguageAndText() {
        GtfsRealtime.TranslatedString result = AlertHandler.toGtfsTranslatedString(
                List.of(translation("fi", "Hei"), translation("sv", "Hej"), translation("en", "Hello")));

        assertEquals(3, result.getTranslationCount());
        assertEquals("fi", result.getTranslation(0).getLanguage());
        assertEquals("Hei", result.getTranslation(0).getText());
        assertEquals("sv", result.getTranslation(1).getLanguage());
        assertEquals("Hej", result.getTranslation(1).getText());
        assertEquals("en", result.getTranslation(2).getLanguage());
        assertEquals("Hello", result.getTranslation(2).getText());
    }

    @Test
    public void translatedStringOfEmptyListIsEmpty() {
        assertEquals(0, AlertHandler.toGtfsTranslatedString(List.of()).getTranslationCount());
    }

    @Test
    public void noServiceIsKeptWhenGlobalNoServiceAlertsAreEnabled() {
        Bulletin b = bulletin().setImpact(Bulletin.Impact.CANCELLED).setAffectsAllRoutes(true).build();
        assertEquals(GtfsRealtime.Alert.Effect.NO_SERVICE, AlertHandler.getGtfsEffect(b, true));
    }

    @Test
    public void noServiceBecomesReducedServiceWhenAllRoutesAreAffectedAndGlobalAlertsDisabled() {
        Bulletin b = bulletin().setImpact(Bulletin.Impact.CANCELLED).setAffectsAllRoutes(true).build();
        assertEquals(GtfsRealtime.Alert.Effect.REDUCED_SERVICE, AlertHandler.getGtfsEffect(b, false));
    }

    @Test
    public void noServiceBecomesReducedServiceWhenAllStopsAreAffectedAndGlobalAlertsDisabled() {
        Bulletin b = bulletin().setImpact(Bulletin.Impact.CANCELLED).setAffectsAllStops(true).build();
        assertEquals(GtfsRealtime.Alert.Effect.REDUCED_SERVICE, AlertHandler.getGtfsEffect(b, false));
    }

    @Test
    public void noServiceIsKeptForTargetedBulletinEvenWhenGlobalAlertsDisabled() {
        Bulletin b = bulletin().setImpact(Bulletin.Impact.CANCELLED).build();
        assertEquals(GtfsRealtime.Alert.Effect.NO_SERVICE, AlertHandler.getGtfsEffect(b, false));
    }

    @Test
    public void otherEffectsAreNotChangedForBulletinsAffectingAll() {
        for (Bulletin.Impact impact : Bulletin.Impact.values()) {
            if (impact == Bulletin.Impact.CANCELLED) {
                continue;
            }
            Bulletin b = bulletin().setImpact(impact).setAffectsAllRoutes(true).setAffectsAllStops(true).build();
            assertEquals(impact.name(), EXPECTED_EFFECTS.get(impact), AlertHandler.getGtfsEffect(b, false));
        }
    }

    @Test
    public void entitySelectorsForAffectedStops() {
        Bulletin b = bulletin().clearAffectedStops().addAffectedStops(entity("1")).addAffectedStops(entity("2"))
                .build();

        Collection<GtfsRealtime.EntitySelector> selectors = AlertHandler.entitySelectorsForBulletin(b);

        assertEquals(Set.of(GtfsRealtime.EntitySelector.newBuilder().setStopId("1").build(),
                GtfsRealtime.EntitySelector.newBuilder().setStopId("2").build()), new HashSet<>(selectors));
    }

    @Test
    public void entitySelectorsAreDeduplicated() {
        Bulletin b = bulletin().clearAffectedStops().addAffectedStops(entity("1")).addAffectedStops(entity("1"))
                .addAffectedRoutes(entity("2550")).addAffectedRoutes(entity("2550")).build();

        assertEquals(2, AlertHandler.entitySelectorsForBulletin(b).size());
    }

    @Test
    public void bulletinAffectingAllRoutesGetsAgencySelectorInAdditionToEntities() {
        Bulletin b = bulletin().setAffectsAllRoutes(true).addAffectedRoutes(entity("1009")).build();

        Collection<GtfsRealtime.EntitySelector> selectors = AlertHandler.entitySelectorsForBulletin(b);

        assertEquals(
                Set.of(GtfsRealtime.EntitySelector.newBuilder().setAgencyId(AlertHandler.AGENCY_ENTITY_SELECTOR)
                        .build(), GtfsRealtime.EntitySelector.newBuilder().setRouteId("1009").build(),
                        GtfsRealtime.EntitySelector.newBuilder().setStopId("1230103").build()),
                new HashSet<>(selectors));
    }

    @Test
    public void bulletinAffectingAllStopsGetsOnlyOneAgencySelector() {
        Bulletin b = bulletin().clearAffectedStops().setAffectsAllRoutes(true).setAffectsAllStops(true).build();

        Collection<GtfsRealtime.EntitySelector> selectors = AlertHandler.entitySelectorsForBulletin(b);

        assertEquals(1, selectors.size());
        assertEquals("HSL", selectors.iterator().next().getAgencyId());
    }

    @Test
    public void createAlertMapsAllFields() {
        Optional<GtfsRealtime.Alert> maybeAlert = AlertHandler.createAlert(bulletin().build(), true);

        assertTrue(maybeAlert.isPresent());
        GtfsRealtime.Alert alert = maybeAlert.get();
        assertEquals(1, alert.getActivePeriodCount());
        // Milliseconds are truncated to whole seconds
        assertEquals(1_700_000_000L, alert.getActivePeriod(0).getStart());
        assertEquals(1_700_003_600L, alert.getActivePeriod(0).getEnd());
        assertEquals(GtfsRealtime.Alert.Cause.CONSTRUCTION, alert.getCause());
        assertEquals(GtfsRealtime.Alert.Effect.DETOUR, alert.getEffect());
        assertEquals(GtfsRealtime.Alert.SeverityLevel.WARNING, alert.getSeverityLevel());
        assertEquals(2, alert.getHeaderText().getTranslationCount());
        assertEquals("Title", alert.getHeaderText().getTranslation(1).getText());
        assertEquals(1, alert.getDescriptionText().getTranslationCount());
        assertEquals(1, alert.getUrl().getTranslationCount());
        assertEquals(List.of(GtfsRealtime.EntitySelector.newBuilder().setStopId("1230103").build()),
                alert.getInformedEntityList());
    }

    @Test
    public void createAlertOmitsTextsThatAreMissing() {
        Bulletin b = bulletin().clearTitles().clearDescriptions().clearUrls().build();

        GtfsRealtime.Alert alert = AlertHandler.createAlert(b, true).get();

        assertFalse(alert.hasHeaderText());
        assertFalse(alert.hasDescriptionText());
        assertFalse(alert.hasUrl());
    }

    @Test
    public void createAlertWithoutPriorityUsesInfoSeverity() {
        // priority is an optional proto2 field whose default is INFO, so an unset priority still maps to INFO
        Bulletin b = bulletin().clearPriority().build();

        GtfsRealtime.Alert alert = AlertHandler.createAlert(b, true).get();

        assertEquals(GtfsRealtime.Alert.SeverityLevel.INFO, alert.getSeverityLevel());
    }

    @Test
    public void createAlertDiscardsBulletinWithoutInformedEntities() {
        Bulletin b = bulletin().clearAffectedStops().clearAffectedRoutes().build();
        assertEquals(Optional.empty(), AlertHandler.createAlert(b, true));
    }

    @Test
    public void createAlertForDisplayOnlyFalseIsCreated() {
        assertTrue(AlertHandler.createAlert(bulletin().setDisplayOnly(false).build(), true).isPresent());
    }

    @Test
    public void createFeedEntitiesSkipsDiscardedBulletinsAndUsesBulletinIdAsEntityId() {
        Bulletin kept = bulletin().setBulletinId("kept").build();
        Bulletin displayOnly = bulletin().setBulletinId("displayOnly").setDisplayOnly(true).build();
        Bulletin noEntities = bulletin().setBulletinId("noEntities").clearAffectedStops().build();
        Bulletin alsoKept = bulletin().setBulletinId("alsoKept").build();

        List<GtfsRealtime.FeedEntity> entities = AlertHandler
                .createFeedEntities(List.of(kept, displayOnly, noEntities, alsoKept), true);

        assertEquals(2, entities.size());
        assertEquals("kept", entities.get(0).getId());
        assertEquals("alsoKept", entities.get(1).getId());
        assertTrue(entities.get(0).hasAlert());
    }

    @Test
    public void createFeedEntitiesOfNoBulletinsIsEmpty() {
        assertTrue(AlertHandler.createFeedEntities(List.<InternalMessages.Bulletin>of(), true).isEmpty());
    }
}
