package lambda;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;

/**
 * Load lambda function as part of the Transform-Load-Query pipeline for TCSS-462.
 * This lambda function is automatically invoked via a Cloud-Watch
 * event when a file is placed in the correct S3 bucket.
 * This function loads the transformed file given by
 * the S3 bucket event into the proper Aurora RDS.
 *
 * @author Brandon Ragghianti
 * @author Michael
 * @author Tyler
 * @author Gabriel Stupart
 * @version 1.0
 */
public class LoadAurora implements RequestHandler<HashMap<String, Object>,
                                                  HashMap<String, Object>> {

    /**
     * Handler for the AWS lambda function. Automatically triggered by a Cloud-Watch event.
     * @param request The generated request from AWS. Must include a bucketname
     *                and filename property for the csv to be loaded.
     * @param context The generated context from AWS.
     * @return The state of this lambda function container.
     */
    public HashMap<String, Object> handleRequest(
            final HashMap<String, Object> request, final Context context
    ) {

        //Collect initial data.
        final Inspector inspector = new Inspector();
        inspector.inspectCPU();
        inspector.inspectMemory();
        inspector.inspectContainer();

        //****************START FUNCTION IMPLEMENTATION*************************

        final LambdaLogger logger = context.getLogger();

        // Retrieve the bucketname and filename from the S3 event JSON.
        final HashMap<?, ?> requestParameters = (HashMap<?, ?>) ((HashMap<?, ?>) request.get("detail")).get("requestParameters");
        final String bucket_name = (String) requestParameters.get("bucketName");
        final String filename = (String) requestParameters.get("key");

        // Retrieve the file from S3.
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        final S3Object s3Object = s3Client.getObject(
                                    new GetObjectRequest(bucket_name, filename));
        final InputStream objectData = s3Object.getObjectContent();

        // Create a CSVParser on the S3 file.
        final CSVParser dataParser;
        try {
            dataParser = CSVParser.parse(objectData, Charset.defaultCharset(), CSVFormat.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

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
            logger.log("Database connection failed: " + e.getMessage());
            throw new RuntimeException(e);
        }

        // Detect if the table 'data' exists in the database
        try {
            final PreparedStatement db_table_create = con.prepareStatement("CREATE TABLE IF NOT EXISTS data (" +
            "userID INTEGER AUTO_INCREMENT, " +
            "userAge REAL, " +
            "userGender TEXT, " +
            "userNumberOfApps INTEGER, " +
            "userSocialMediaUsage REAL, " +
            "userPercentOfSocialMedia REAL, " +
            "userProductivityAppUsage REAL, " +
            "userPercentOfProductivityAppUsage REAL, " +
            "userGamingAppUsage REAL, " +
            "userPercentOfGamingAppUsage REAL, " +
            "userTotalAppUsage REAL, " +
            "userCity TEXT, " +
            "resultState TEXT, " +
            "resultCountry TEXT, " +
            "PRIMARY KEY (userID)" +
            ")");
            db_table_create.executeUpdate();
            db_table_create.close();
        } catch (final SQLException e) {
            logger.log("Failed to check/create the database data table: " + e.getMessage());
            throw new RuntimeException(e);
        }

        // Insert all data into the database.
        try {
            final PreparedStatement db_table_insert = con.prepareStatement("INSERT INTO data (userAge, userGender, userNumberOfApps, userSocialMediaUsage, userPercentOfSocialMedia, userProductivityAppUsage, userPercentOfProductivityAppUsage, userGamingAppUsage, userPercentOfGamingAppUsage, userTotalAppUsage, userCity, resultState, resultCountry) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");

            int batchSize = 1000; // Adjust the batch size based on your system's capability
            int count = 0;

            for (CSVRecord csvRecord : dataParser) {
                int paramIndex = 1;
                paramIndex = substitute(db_table_insert, paramIndex, 0, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 1, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 2, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 3, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 4, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 5, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 6, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 7, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 8, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 9, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 10, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 11, csvRecord);
                paramIndex = substitute(db_table_insert, paramIndex, 12, csvRecord);

                db_table_insert.addBatch();
                count++;

                if (count % batchSize == 0) {
                    db_table_insert.executeBatch();
                }
            }
            // Execute any remaining batches
            db_table_insert.executeBatch();
        } catch (final SQLException e) {
            logger.log("Failed to insert data: " + e.getMessage());
            throw new RuntimeException(e);
        }

        // Delete S3 file.
        s3Client.deleteObject(new DeleteObjectRequest(bucket_name, filename));

        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private int substitute(final PreparedStatement db_table_insert, int count, int pos, CSVRecord csvRecord) throws SQLException {
        db_table_insert.setString(count, csvRecord.get(pos));
        count++;
        return count;
    }
}
