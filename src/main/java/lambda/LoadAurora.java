package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
        inspector.inspectAll();

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
            final PreparedStatement db_table_check = con.prepareStatement("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'mobiledata' AND table_name = 'data');");
            final ResultSet db_table_rs = db_table_check.executeQuery();
            db_table_rs.next();
            if (!db_table_rs.getBoolean(1)) {
                final PreparedStatement db_table_create = con.prepareStatement(
                        "CREATE TABLE data (userID INTEGER AUTO_INCREMENT, userAge INTEGER, userGender TEXT, userNumberOfApps INTEGER, "
                        + "userSocialMediaUsage INTEGER, userPercentOfSocialMedia REAL, userProductivityAppUsage REAL, "
                        + "userPercentOfProductivityAppUsage REAL, userGamingAppUsage REAL, userPercentOfGamingAppUsage REAL, "
                        + "userCity TEXT, resultState TEXT, resultCountry TEXT, PRIMARY KEY (userID));");
                db_table_create.execute();
                db_table_create.close();
            }
            db_table_check.close();
            db_table_rs.close();
        } catch (final SQLException e) {
            logger.log("Failed to check/create the database data table: " + e.getMessage());
            throw new RuntimeException(e);
        }

        // Insert all data into the database.
        try {
            final PreparedStatement db_table_insert = con.prepareStatement("INSERT INTO data (userAge, userGender, userNumberOfApps, userSocialMediaUsage, userPercentOfSocialMedia, userProductivityAppUsage, userPercentOfProductivityAppUsage, userGamingAppUsage, userPercentOfGamingAppUsage, userCity, resultState, resultCountry) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
            for (CSVRecord csvRecord : dataParser) {
                db_table_insert.setString(1, csvRecord.get(0));
                db_table_insert.setString(2, csvRecord.get(1));
                db_table_insert.setString(3, csvRecord.get(2));
                db_table_insert.setString(4, csvRecord.get(3));
                db_table_insert.setString(5, csvRecord.get(4));
                db_table_insert.setString(6, csvRecord.get(5));
                db_table_insert.setString(7, csvRecord.get(6));
                db_table_insert.setString(8, csvRecord.get(7));
                db_table_insert.setString(9, csvRecord.get(8));
                db_table_insert.setString(10, csvRecord.get(9));
                db_table_insert.setString(11, csvRecord.get(10));
                db_table_insert.setString(12, csvRecord.get(11));
            }
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
}
