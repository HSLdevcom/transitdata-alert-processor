package fi.hsl.transitdata.omm.db;

import fi.hsl.transitdata.omm.models.Bulletin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class BulletinDAOImpl extends DAOImplBase implements BulletinDAO {

    String queryString;
    String timezone;

    public BulletinDAOImpl(Connection connection, String timezone) {
        super(connection);
        this.timezone = timezone;
        queryString = createQuery();
    }

    @Override
    public List<Bulletin> getActiveBulletins() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(queryString)) {
            String now = localDatetimeAsString(timezone);
            statement.setString(1, now);

            ResultSet results = performQuery(statement);
            return parseBulletins(results);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing Routes", e);
            throw e;
        }
    }

    private List<Bulletin> parseBulletins(ResultSet resultSet) throws SQLException {
        List<Bulletin> bulletins = new LinkedList<>();
        while (resultSet.next()) {
            Bulletin bulletin = new Bulletin();

            bulletin.id = resultSet.getLong("bulletins_id");
            bulletin.category = Bulletin.Category.fromString(resultSet.getString("category"));
            bulletin.lastModified = LocalDateTime.parse(resultSet.getString("last_modified"));
            bulletin.validFrom = LocalDateTime.parse(resultSet.getString("valid_from"));
            bulletin.validTo = LocalDateTime.parse(resultSet.getString("valid_to"));
            bulletin.affectsAllRoutes = resultSet.getInt("affects_all_routes") > 0;
            bulletin.affectsAllStops = resultSet.getInt("affects_all_stops") > 0;
            bulletin.affectedRouteIds = parseListFromCommaSeparatedString(resultSet.getString("affected_route_ids"));
            bulletin.affectedStopIds = parseListFromCommaSeparatedString(resultSet.getString("affected_stop_ids"));
            bulletin.textFi = parseLocalization(resultSet, Bulletin.Language.FI);
            bulletin.textEn = parseLocalization(resultSet, Bulletin.Language.EN);
            bulletin.textSv = parseLocalization(resultSet, Bulletin.Language.SV);

            bulletins.add(bulletin);
        }
        return bulletins;
    }

    Bulletin.LocalizedText parseLocalization(ResultSet resultSet, Bulletin.Language language) throws SQLException {
        String columnSuffix = "";
        switch (language) {
            case FI:
                columnSuffix = "_fi";
                break;
            case EN:
                columnSuffix = "_en";
                break;
            case SV:
                columnSuffix = "_sv";
                break;
        }

        String title = resultSet.getString("title" + columnSuffix);
        String text = resultSet.getString("text" + columnSuffix);

        Bulletin.LocalizedText localization = new Bulletin.LocalizedText();
        localization.language = language;
        localization.title = title;
        localization.text = text;
        return localization;
    }

    List<Long> parseListFromCommaSeparatedString(String value) {
        if (value != null && !value.isEmpty()) {
            return Arrays.stream(value.split(","))
                    .map(subString -> Long.parseLong(subString.trim()))
                    .collect(Collectors.toList());
        } else {
            return new LinkedList<>();
        }
    }


    private String createQuery() {
        return "SELECT B.bulletins_id" +
                "    ,B.category" +
                "    ,B.last_modified" +
                "    ,B.valid_from" +
                "    ,B.valid_to" +
                "    ,B.affects_all_routes" +
                "    ,B.affects_all_stops" +
                "    ,B.affected_route_ids" +
                "    ,B.affected_stop_ids" +
                "    ,MAX(CASE WHEN BLM.language_code = 'fi' THEN BLM.title END) AS title_fi" +
                "    ,MAX(CASE WHEN BLM.language_code = 'fi' THEN BLM.description END) AS text_fi" +
                "    ,MAX(CASE WHEN BLM.language_code = 'sv' THEN BLM.title END) AS title_sv" +
                "    ,MAX(CASE WHEN BLM.language_code = 'sv' THEN BLM.description END) AS text_sv" +
                "    ,MAX(CASE WHEN BLM.language_code = 'en' THEN BLM.title END) AS title_en" +
                "    ,MAX(CASE WHEN BLM.language_code = 'en' THEN BLM.description END) AS text_en" +
                "  FROM [OMM_Community].[dbo].[bulletins] AS B" +
                "    LEFT JOIN [OMM_Community].[dbo].bulletin_localized_messages AS BLM ON BLM.bulletins_id = B.bulletins_id" +
                "  WHERE B.[type] = 'PASSENGER_INFORMATION' " +
                "    AND B.valid_to > ?" +
                "  GROUP BY B.bulletins_id" +
                "    ,B.category" +
                "    ,B.last_modified" +
                "    ,B.valid_from" +
                "    ,B.valid_to" +
                "    ,B.affects_all_routes" +
                "    ,B.affects_all_stops" +
                "    ,B.affected_route_ids" +
                "    ,B.affected_stop_ids";
    }
}
