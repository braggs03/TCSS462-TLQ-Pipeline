package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;
import saaf.Inspector;

/**
 * Query lambda function as part of the Transform-Load-Query pipeline for TCSS-462.
 * This lambda function is invoked via a RESTful API Gateway.
 * This function queries the 'data' table in the database specified by db.properties.
 * The result is returned to the user along with CPU and Memory Deltas.
 *
 * @author Brandon Ragghianti
 * @author Michael
 * @author Tyler
 * @author Gabriel Stupart
 * @version 1.0
 */
public class Query implements RequestHandler<HashMap<String, Object>,
                                             HashMap<String, Object>> {

    /**
     * Handler for the AWS lambda function. Automatically triggered by a Cloud-Watch event.
     * @param request The generated request from AWS. Must include a bucketname and filename property for the csv to be loaded.
     * @param context The generated context from AWS.
     * @return The state of this lambda function container.
     */
    public HashMap<String, Object> handleRequest(
            final HashMap<String, Object> request,
            final Context context
    ) {

        //Collect initial data.
        final Inspector inspector = new Inspector();
        inspector.inspectCPU();
        inspector.inspectMemory();
        inspector.inspectContainer();

        //****************START FUNCTION IMPLEMENTATION*************************

        final LambdaLogger logger = context.getLogger();

        // Turn AWS request object to proper json.
        final JSONObject jsonRequest = new JSONObject(request);

        // Step 1: Parse the JSON to receive aggregations and filters.
        final JSONArray aggregations = jsonRequest.optJSONArray("aggregations");
        final JSONArray filters = jsonRequest.optJSONArray("filters");
        final JSONArray group = jsonRequest.optJSONArray("group");

        // Create the start of the SQL query.
        final StringBuilder sqlQuery = new StringBuilder("SELECT ");

        // Step 2: Parse the aggregations part of the json and add to select.
        if (group != null && !group.isEmpty()) {
            for (int i = 0; i < group.length(); i++) {
                final String value = group.getString(i);
                sqlQuery.append(value).append(", ");
            }
        }

        if (aggregations != null && !aggregations.isEmpty())  {
            for (int i = 0; i < aggregations.length(); i++) {
                final JSONObject aggregation = aggregations.getJSONObject(i);
                final String column = aggregation.getString("column");
                final String function = aggregation.getString("function");
                sqlQuery.append(function).append("(").append(column).append(") AS ").append(function).append("_").append(column);
                if (i != aggregations.length() - 1) {
                    sqlQuery.append(", ");
                }
            }
        } else {
            sqlQuery.append(" * ");
        }

        sqlQuery.append(" FROM data ");

        // Create ArrayList to store values instead of directly adding to query to prevent SQL injection.
        final ArrayList<String> values = new ArrayList<>();

        // Step 3: Build the WHERE clause (with Filters)
        if (filters != null && !filters.isEmpty()) {
            sqlQuery.append("WHERE ");
            for (int i = 0; i < filters.length(); i++) {
                final JSONObject filter = filters.getJSONObject(i);
                final String column = filter.getString("column");
                values.add(filter.getString("value"));
                sqlQuery.append(column).append(" = ?");
                if (i != filters.length() - 1) {
                    sqlQuery.append(" AND ");
                }
            }
        }

        if (group != null && !group.isEmpty()) {
            sqlQuery.append(" GROUP BY ");
            for (int i = 0; i < group.length(); i++) {
                final String value = group.getString(i);
                sqlQuery.append(value);
                if (i != group.length() - 1) {
                    sqlQuery.append(", ");
                }
            }
        }

        sqlQuery.append(";");

        System.out.println(sqlQuery.toString());

        // Load db.properties and the required properties.
        final Properties properties = new Properties();
        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("db.properties"));
        } catch (final IOException e) {
            logger.log("Failed to load db.properties: " + e.getMessage());
            throw new RuntimeException(e);
        }
        final String url = properties.getProperty("url");
        final String username = properties.getProperty("username");
        final String password = properties.getProperty("password");

        // Connect to the database.
        final Connection con;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (final SQLException e) {
            System.out.println("Error connecting to " + url);
            throw new RuntimeException(e);
        }

        final ArrayList<HashMap<String, Object>> jsonResult = new ArrayList<>();
        try {
            final PreparedStatement dbTableSelect = con.prepareStatement(sqlQuery.toString());
            for (int i = 0; i < values.size(); i++) {
                dbTableSelect.setString(i + 1, values.get(i));
            }
            final ResultSet rs = dbTableSelect.executeQuery();
            final ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                final int numColumns = rsmd.getColumnCount();
                final HashMap<String, Object> row = new HashMap<>();
                for (int i = 1; i <= numColumns; i++) {
                    final String columnName = rsmd.getColumnName(i);
                    row.put(columnName, rs.getObject(columnName));
                }
                jsonResult.add(row);
            }
        } catch (final SQLException e) {
            System.out.println("Failed to query database:");
            throw new RuntimeException(e);
        }
        inspector.addAttribute("entries", jsonResult);

        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
