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
        producer = context.getSingleProducer();
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

    private static boolean bulletinAffectsAll(InternalMessages.Bulletin bulletin) {
        return bulletin.getAffectsAllRoutes() || bulletin.getAffectsAllStops();
    }

    static Optional<GtfsRealtime.Alert> createAlert(final InternalMessages.Bulletin bulletin) {
        Optional<GtfsRealtime.Alert> maybeAlert;
        try {
            if (bulletin.hasDisplayOnly() && bulletin.getDisplayOnly()) {
                log.debug("No alert created for bulletin {} that is meant to be published only on vehicle displays", bulletin.getBulletinId());
                return Optional.empty();
            }

            final long startInUtcSecs = bulletin.getValidFromUtcMs() / 1000;
            final long stopInUtcSecs = bulletin.getValidToUtcMs() / 1000;
            final GtfsRealtime.TimeRange timeRange = GtfsRealtime.TimeRange.newBuilder()
                    .setStart(startInUtcSecs)
                    .setEnd(stopInUtcSecs)
                    .build();

            final GtfsRealtime.Alert.Builder builder = GtfsRealtime.Alert.newBuilder();
            builder.addActivePeriod(timeRange);
            builder.setCause(toGtfsCause(bulletin.getCategory()));
            builder.setEffect(getGtfsEffect(bulletin));
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
        } catch (Exception e) {
            log.error("Exception while creating an alert for bulletin {}!", bulletin.getBulletinId(), e);
            maybeAlert = Optional.empty();
        }

        maybeAlert.ifPresent(alert -> {
            final Optional<String> titleEn = alert.getHeaderText().getTranslationList().stream().filter(translation -> "en".equals(translation.getLanguage())).findAny().map(GtfsRealtime.TranslatedString.Translation::getText);
            log.info("Created an alert with title {} for bulletin {}", titleEn.orElse("null"), bulletin.getBulletinId());
        });

        return maybeAlert;
    }

    static Collection<GtfsRealtime.EntitySelector> entitySelectorsForBulletin(final InternalMessages.Bulletin bulletin) {
        Set<GtfsRealtime.EntitySelector> selectors = new HashSet<>();
        if (bulletinAffectsAll(bulletin)) {
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
            log.info("Produced a new GTFS-RT service alert message with timestamp {}", timestampMs);
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
            case OTHER_DRIVER_ERROR:
            case TOO_MANY_PASSENGERS:
            case MISPARKED_VEHICLE:
            case TEST:
            case STATE_VISIT:
            case TRACK_BLOCKED:
            case EARLIER_DISRUPTION:
            case OTHER:
            case NO_TRAFFIC_DISRUPTION:
            case TRAFFIC_JAM:
            case PUBLIC_EVENT:
            case STAFF_DEFICIT:
            case DISTURBANCE:
                return GtfsRealtime.Alert.Cause.OTHER_CAUSE;
            case ITS_SYSTEM_ERROR:
            case SWITCH_FAILURE:
            case TECHNICAL_FAILURE:
            case VEHICLE_BREAKDOWN:
            case POWER_FAILURE:
            case VEHICLE_DEFICIT:
                return GtfsRealtime.Alert.Cause.TECHNICAL_PROBLEM;
            case STRIKE:
                return GtfsRealtime.Alert.Cause.STRIKE;
            case VEHICLE_OFF_THE_ROAD:
            case TRAFFIC_ACCIDENT:
            case ACCIDENT:
                return GtfsRealtime.Alert.Cause.ACCIDENT;
            case SEIZURE:
            case MEDICAL_INCIDENT:
                return GtfsRealtime.Alert.Cause.MEDICAL_EMERGENCY;
            case WEATHER:
            case WEATHER_CONDITIONS:
                return GtfsRealtime.Alert.Cause.WEATHER;
            case ROAD_MAINTENANCE:
            case TRACK_MAINTENANCE:
                return GtfsRealtime.Alert.Cause.MAINTENANCE;
            case ROAD_CLOSED:
            case ROAD_TRENCH:
                return GtfsRealtime.Alert.Cause.CONSTRUCTION;
            case ASSAULT:
                return GtfsRealtime.Alert.Cause.POLICE_ACTIVITY;
            default:
                return GtfsRealtime.Alert.Cause.UNKNOWN_CAUSE;
        }
    }

    public static GtfsRealtime.Alert.Effect getGtfsEffect(final InternalMessages.Bulletin bulletin) {
        final boolean affectsAll = bulletinAffectsAll(bulletin);
        final InternalMessages.Bulletin.Impact impact = bulletin.getImpact();

        final GtfsRealtime.Alert.Effect effect = toGtfsEffect(impact);
        if (effect == GtfsRealtime.Alert.Effect.NO_SERVICE && affectsAll) {
            //If the bulletin affects all traffic (i.e. entity selector list contains agency), we don't want to use NO_SERVICE effect, because otherwise Google and others will display all traffic as cancelled
            return GtfsRealtime.Alert.Effect.REDUCED_SERVICE;
        }

        return effect;
    }

    public static GtfsRealtime.Alert.Effect toGtfsEffect(final InternalMessages.Bulletin.Impact impact) {
        switch (impact) {
            case CANCELLED:
                return GtfsRealtime.Alert.Effect.NO_SERVICE;
            case DELAYED:
            case IRREGULAR_DEPARTURES:
                return GtfsRealtime.Alert.Effect.SIGNIFICANT_DELAYS;
            case DEVIATING_SCHEDULE:
            case POSSIBLE_DEVIATIONS:
                return GtfsRealtime.Alert.Effect.MODIFIED_SERVICE;
            case DISRUPTION_ROUTE:
                return GtfsRealtime.Alert.Effect.DETOUR;
            case POSSIBLY_DELAYED:
            case VENDING_MACHINE_OUT_OF_ORDER:
            case RETURNING_TO_NORMAL:
            case OTHER:
                return GtfsRealtime.Alert.Effect.OTHER_EFFECT;
            case REDUCED_TRANSPORT:
                return GtfsRealtime.Alert.Effect.REDUCED_SERVICE;
            case NO_TRAFFIC_IMPACT:
                return GtfsRealtime.Alert.Effect.NO_EFFECT;
            default:
                return GtfsRealtime.Alert.Effect.UNKNOWN_EFFECT;
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
