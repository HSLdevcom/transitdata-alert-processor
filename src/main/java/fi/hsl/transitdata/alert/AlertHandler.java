package fi.hsl.transitdata.alert;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class AlertHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(AlertHandler.class);

    public static final String AGENCY_ENTITY_SELECTOR = "HSL";

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    public AlertHandler(final PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();
    }

    public void handleMessage(final Message message) {
        try {
            if (!TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert)) {
                throw new Exception("Invalid protobuf schema");
            }
            InternalMessages.ServiceAlert alert = InternalMessages.ServiceAlert.parseFrom(message.getData());

            final long timestampMs = message.getEventTime();
            final long timestampSecs = timestampMs / 1000;

            List<GtfsRealtime.FeedEntity> entities = createFeedEntities(alert.getBulletinsList());
            GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createFullFeedMessage(entities, timestampSecs);

            sendPulsarMessage(feedMessage, timestampMs);
        } catch (final Exception e) {
            log.error("Exception while handling message", e);
        } finally {
            ack(message.getMessageId());
        }
    }

    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {});
    }

    static List<GtfsRealtime.FeedEntity> createFeedEntities(final List<InternalMessages.Bulletin> bulletins) {
        return bulletins.stream().map(bulletin -> {
            final Optional<GtfsRealtime.Alert> maybeAlert = createAlert(bulletin);
            return maybeAlert.map(alert -> {
                GtfsRealtime.FeedEntity.Builder builder = GtfsRealtime.FeedEntity.newBuilder();
                builder.setId(bulletin.getBulletinId());
                builder.setAlert(alert);
                return builder.build();
            });
        }).filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    static Optional<GtfsRealtime.Alert> createAlert(final InternalMessages.Bulletin bulletin) {
        Optional<GtfsRealtime.Alert> maybeAlert;
        try {
            final long startInUtcSecs = bulletin.getValidFromUtcMs() / 1000;
            final long stopInUtcSecs = bulletin.getValidToUtcMs() / 1000;
            final GtfsRealtime.TimeRange timeRange = GtfsRealtime.TimeRange.newBuilder()
                    .setStart(startInUtcSecs)
                    .setEnd(stopInUtcSecs)
                    .build();

            final GtfsRealtime.Alert.Builder builder = GtfsRealtime.Alert.newBuilder();
            builder.addActivePeriod(timeRange);
            builder.setCause(toGtfsCause(bulletin.getCategory()));
            builder.setEffect(toGtfsEffect(bulletin.getImpact()));
            if (bulletin.getTitlesCount() > 0) {
                builder.setHeaderText(toGtfsTranslatedString(bulletin.getTitlesList()));
            }
            if (bulletin.getDescriptionsCount() > 0) {
                builder.setDescriptionText(toGtfsTranslatedString(bulletin.getDescriptionsList()));
            }
            if (bulletin.getUrlsCount() > 0) {
                builder.setUrl(toGtfsTranslatedString(bulletin.getUrlsList()));
            }
            final Optional<GtfsRealtime.Alert.SeverityLevel> maybeSeverityLevel = toGtfsSeverityLevel(bulletin.getPriority());
            maybeSeverityLevel.ifPresent(builder::setSeverityLevel);

            Collection<GtfsRealtime.EntitySelector> entitySelectors = entitySelectorsForBulletin(bulletin);
            if (entitySelectors.isEmpty()) {
                log.error("Failed to find any Informed Entities for bulletin Id {}. Discarding alert.", bulletin.getBulletinId());
                maybeAlert = Optional.empty();
            }
            else {
                builder.addAllInformedEntity(entitySelectors);
                maybeAlert = Optional.of(builder.build());
            }
        }
        catch (Exception e) {
            log.error("Exception while creating an alert!", e);
            maybeAlert = Optional.empty();
        }
        return maybeAlert;
    }

    static Collection<GtfsRealtime.EntitySelector> entitySelectorsForBulletin(final InternalMessages.Bulletin bulletin) {
        Set<GtfsRealtime.EntitySelector> selectors = new HashSet<>();
        if (bulletin.getAffectsAllRoutes() || bulletin.getAffectsAllStops()) {
            log.debug("Bulletin {} affects all routes or stops", bulletin.getBulletinId());

            GtfsRealtime.EntitySelector agency = GtfsRealtime.EntitySelector.newBuilder()
                    .setAgencyId(AGENCY_ENTITY_SELECTOR)
                    .build();
            selectors.add(agency);
        }
        if (bulletin.getAffectedRoutesCount() > 0) {
            for (final InternalMessages.Bulletin.AffectedEntity route : bulletin.getAffectedRoutesList()) {
                GtfsRealtime.EntitySelector entity = GtfsRealtime.EntitySelector.newBuilder()
                        .setRouteId(RouteIdUtils.normalizeRouteId(route.getEntityId())) //Normalize route ID to avoid publishing IDs that are not present in the static feed
                        .build();
                selectors.add(entity);
            }
        }
        if (bulletin.getAffectedStopsCount() > 0) {
            for (final InternalMessages.Bulletin.AffectedEntity stop : bulletin.getAffectedStopsList()) {
                GtfsRealtime.EntitySelector entity = GtfsRealtime.EntitySelector.newBuilder()
                        .setStopId(stop.getEntityId()).build();
                selectors.add(entity);
            }
        }

        return selectors;
    }

    private void sendPulsarMessage(final GtfsRealtime.FeedMessage feedMessage, long timestampMs) throws PulsarClientException {
        try {
            producer.newMessage().value(feedMessage.toByteArray())
                    .eventTime(timestampMs)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_ServiceAlert.toString())
                    .send();
            log.info("Produced a new alert with timestamp {}", timestampMs);
        }
        catch (PulsarClientException e) {
            log.error("Failed to send message to Pulsar", e);
            throw e;
        }
        catch (Exception e) {
            log.error("Failed to handle alert message", e);
        }
    }

    public static GtfsRealtime.Alert.Cause toGtfsCause(final InternalMessages.Category category) {
        switch (category) {
            case OTHER_DRIVER_ERROR: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case ITS_SYSTEM_ERROR: return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            case TOO_MANY_PASSENGERS: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case MISPARKED_VEHICLE: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case STRIKE: return GtfsRealtime.Alert.Cause.STRIKE;
            case TEST: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case VEHICLE_OFF_THE_ROAD: return GtfsRealtime.Alert.Cause.ACCIDENT;
            case TRAFFIC_ACCIDENT: return GtfsRealtime.Alert.Cause.ACCIDENT;
            case SWITCH_FAILURE: return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            case SEIZURE: return GtfsRealtime.Alert.Cause.MEDICAL_EMERGENCY;
            case WEATHER: return GtfsRealtime.Alert.Cause.WEATHER;
            case STATE_VISIT: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case ROAD_MAINTENANCE: return GtfsRealtime.Alert.Cause.MAINTENANCE;
            case ROAD_CLOSED: return GtfsRealtime.Alert.Cause.CONSTRUCTION;
            case TRACK_BLOCKED: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case WEATHER_CONDITIONS: return GtfsRealtime.Alert.Cause.WEATHER;
            case ASSAULT: return GtfsRealtime.Alert.Cause.POLICE_ACTIVITY;
            case TRACK_MAINTENANCE: return GtfsRealtime.Alert.Cause.MAINTENANCE;
            case MEDICAL_INCIDENT: return GtfsRealtime.Alert.Cause.MEDICAL_EMERGENCY;
            case EARLIER_DISRUPTION: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case TECHNICAL_FAILURE: return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            case TRAFFIC_JAM: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case OTHER: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case NO_TRAFFIC_DISRUPTION: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case ACCIDENT: return GtfsRealtime.Alert.Cause.ACCIDENT;
            case PUBLIC_EVENT: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case ROAD_TRENCH: return GtfsRealtime.Alert.Cause.CONSTRUCTION;
            case VEHICLE_BREAKDOWN: return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            case POWER_FAILURE: return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            case STAFF_DEFICIT: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case DISTURBANCE: return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case VEHICLE_DEFICIT: return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            default: return GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE;
        }
    }

    public static GtfsRealtime.Alert.Effect toGtfsEffect(final InternalMessages.Bulletin.Impact impact) {
        switch (impact) {
            case CANCELLED: return GtfsRealtime.Alert.Effect.NO_SERVICE;
            case DELAYED: return GtfsRealtime.Alert.Effect.SIGNIFICANT_DELAYS;
            case DEVIATING_SCHEDULE: return GtfsRealtime.Alert.Effect.MODIFIED_SERVICE;
            case DISRUPTION_ROUTE: return GtfsRealtime.Alert.Effect.DETOUR;
            case IRREGULAR_DEPARTURES: return GtfsRealtime.Alert.Effect.SIGNIFICANT_DELAYS;
            case POSSIBLE_DEVIATIONS: return GtfsRealtime.Alert.Effect.MODIFIED_SERVICE;
            case POSSIBLY_DELAYED: return GtfsRealtime.Alert.Effect.OTHER_EFFECT;
            case REDUCED_TRANSPORT: return GtfsRealtime.Alert.Effect.REDUCED_SERVICE;
            case RETURNING_TO_NORMAL: return GtfsRealtime.Alert.Effect.OTHER_EFFECT;
            case VENDING_MACHINE_OUT_OF_ORDER: return GtfsRealtime.Alert.Effect.OTHER_EFFECT;
            case NULL: return GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT;
            case OTHER: return GtfsRealtime.Alert.Effect.OTHER_EFFECT;
            case NO_TRAFFIC_IMPACT: return GtfsRealtime.Alert.Effect.NO_EFFECT;
            case UNKNOWN: return GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT;
            default: return GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT;
        }
    }

    public static Optional<GtfsRealtime.Alert.SeverityLevel> toGtfsSeverityLevel(final InternalMessages.Bulletin.Priority priority) {
        switch (priority) {
            case INFO: return Optional.of(GtfsRealtime.Alert.SeverityLevel.INFO);
            case WARNING: return Optional.of(GtfsRealtime.Alert.SeverityLevel.WARNING);
            case SEVERE: return Optional.of(GtfsRealtime.Alert.SeverityLevel.SEVERE);
            default: return Optional.empty();
        }
    }

    public static GtfsRealtime.TranslatedString toGtfsTranslatedString(final List<InternalMessages.Bulletin.Translation> translations) {
        GtfsRealtime.TranslatedString.Builder builder = GtfsRealtime.TranslatedString.newBuilder();
        for (final InternalMessages.Bulletin.Translation translation: translations) {
            GtfsRealtime.TranslatedString.Translation gtfsTranslation = GtfsRealtime.TranslatedString.Translation.newBuilder()
                    .setText(translation.getText())
                    .setLanguage(translation.getLanguage())
                    .build();
            builder.addTranslation(gtfsTranslation);
        }
        return builder.build();
    }
}
