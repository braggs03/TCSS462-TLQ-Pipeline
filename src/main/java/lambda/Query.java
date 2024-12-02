package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import saaf.Inspector;

public class Query implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    /**
     * Handler for the AWS lambda function. Automatically triggered by a Cloud-Watch event.
     * @param request The generated request from AWS. Must include a bucketname and filename property for the csv to be loaded.
     * @param context The generated context from AWS.
     * @return The state of this lambda function container.
     */
    public HashMap<String, Object> handleRequest(
            final HashMap<String, Object> request, final Context context
    ) {

        //Collect initial data.
        final Inspector inspector = new Inspector();
        inspector.inspectAll();

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
                values.add(value);
                sqlQuery.append("?");
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

        final ArrayList<JSONObject> result = new ArrayList<>();
        try {
            final PreparedStatement dbTableSelect = con.prepareStatement(sqlQuery.toString());
            for (int i = 0; i < values.size(); i++) {
                dbTableSelect.setString(i + 1, values.get(i));
            }
            final ResultSet rs = dbTableSelect.executeQuery();
            final ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                final int numColumns = rsmd.getColumnCount();
                final JSONObject obj = new JSONObject();
                for (int i = 1; i <= numColumns; i++) {
                    final String columnName = rsmd.getColumnName(i);
                    obj.put(columnName, rs.getObject(columnName));
                }
                logger.log(obj.toString());
                result.add(obj);
            }
        } catch (final SQLException e) {
            System.out.println("Failed to query database:");
            throw new RuntimeException(e);
        }
        inspector.addAttribute("entries", result);

        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    public static void main(final String[] args) throws FileNotFoundException {

        Scanner scanner = new Scanner(new File("json.json"));
        String request = "";
        while (scanner.hasNextLine()) {
            request += scanner.nextLine();
        }

        // Turn AWS request object to proper json.
        final JSONObject jsonRequest = new JSONObject(request);

        // Step 1: Parse the JSON to receive aggregations and filters.
        final JSONArray aggregations = jsonRequest.optJSONArray("aggregations");
        final JSONArray filters = jsonRequest.optJSONArray("filters");
        final JSONArray group = jsonRequest.optJSONArray("group");

        // Create the start of the SQL query.
        final StringBuilder sqlQuery = new StringBuilder("SELECT ");

        // Step 2: Parse the aggregations part of the json and add to select.
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
                values.add(value);
                sqlQuery.append("?");
                if (i != group.length() - 1) {
                    sqlQuery.append(", ");
                }
            }
        }

        sqlQuery.append(";");

        System.out.println(sqlQuery.toString());

        System.out.println(values.toString());
    }
}
