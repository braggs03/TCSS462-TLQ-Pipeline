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

public class LoadAurora implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    /**
     * Handler for the AWS lambda function. Automatically triggered by a Cloud-Watch event.
     * @param request The generated request from AWS. Must include a bucketname and filename property for the csv to be loaded.
     * @param context The generated context from AWS.
     * @return The state of this lambda function container.
     */
    public HashMap<String, Object> handleRequest(
            final HashMap<String, Object> request, final Context context
    ) {
        LambdaLogger logger = context.getLogger();

        //Collect initial data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************

        // Retrieve the bucketname and filename from the S3 event JSON.
        final HashMap<?, ?> requestParameters = (HashMap<?, ?>) ((HashMap<?, ?>) request.get("detail")).get("requestParameters");
        final String bucketname = (String) requestParameters.get("bucketName");
        final String filename = (String) requestParameters.get("key");

        // Retrieve and the access the file from S3.
        logger.log("Bucket Name: " + bucketname);
        logger.log("File Name: " + filename);
        System.out.println("Bucket Name: " + bucketname);
        System.out.println("File Name: " + filename);

        AmazonS3 s3Client = null;
        try {
            logger.log("Initializing Amazon S3 client...");
            s3Client = AmazonS3ClientBuilder.standard().build();
            logger.log("Amazon S3 client initialized successfully.");
        } catch (Exception e) {
            logger.log("Failed to initialize Amazon S3 client: " + e.getMessage());
            throw e;
        }
        logger.log("trying to get object");
        final S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        logger.log("trying to get object data");
        final InputStream objectData = s3Object.getObjectContent();


        logger.log("trying to parse csv");
        // Create a CSVParser on the S3 file.
        final CSVParser dataParser;
        try {
            dataParser = CSVParser.parse(objectData, Charset.defaultCharset(), CSVFormat.DEFAULT);
            logger.log("csv parsed");
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        
        try
        {
            logger.log("Inside try catch block.");
            Properties properties = new Properties();
            logger.log("opening dp properties.");
            properties.load(LoadAurora.class.getClassLoader().getResourceAsStream("/db.properties"));
            logger.log("url.");
            String url = properties.getProperty("url");
            logger.log("username.");
            String username = properties.getProperty("username");
            logger.log("password.");
            String password = properties.getProperty("password");
            Connection con = null;
            try {
                logger.log("trying to connect to db.");
                con = DriverManager.getConnection(url, username, password);
                logger.log("Connected to DB");
            } catch (SQLException e) {
                logger.log("Connect failed: " + e.getMessage());
                System.out.println("Connect failed: " + e.getMessage());
            }

            // Detect if the table 'data' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'mobiledata' AND table_name = 'data');");
            ResultSet rs = ps.executeQuery();
            rs.next();
            if (!rs.getBoolean(1))
            {
                // 'data' does not exist, and should be created
                logger.log("trying to create table 'data'");
                System.out.println("trying to create table 'data'");
                ps.close();
                ps = con.prepareStatement("CREATE TABLE data (userID INTEGER AUTO_INCREMENT, userAge INTEGER, userGender TEXT, userNumberOfApps INTEGER, " + 
                "userSocialMediaUsage INTEGER, userPercentOfSocialMedia REAL, userProductivityAppUsage REAL, " +
                "userPercentOfProductivityAppUsage REAL, userGamingAppUsage REAL, userPercentOfGamingAppUsage REAL, " +
                "userCity TEXT, resultState TEXT, resultCountry TEXT, PRIMARY KEY (userID));");
                ps.execute();
                logger.log("created table 'data'");
                System.out.println("created table 'data'");
            }
            rs.close();

            for (CSVRecord csvRecord : dataParser) {    
                // Insert row into 'data' (pattern of filled in variables done by ChatGPT)
                ps.close();
                ps = con.prepareStatement("INSERT INTO data VALUES ("
                    + "NULL, "                                              // Primary key
                    + csvRecord.get(0) + ", '"                  // User age (INTEGER)
                    + csvRecord.get(1) + "', "               // User gender (TEXT)
                    + csvRecord.get(2) + ", "          // Number of apps (INTEGER)
                    + csvRecord.get(3) + ", "      // Social media usage (INTEGER)
                    + csvRecord.get(4) + ", "  // Percent of social media usage (REAL)
                    + csvRecord.get(5) + ", "  // Productivity app usage (REAL)
                    + csvRecord.get(6) + ", "  // Percent of productivity app usage (REAL)
                    + csvRecord.get(7) + ", "        // Gaming app usage (REAL)
                    + csvRecord.get(8) + ", "  // Percent of gaming app usage (REAL)
                    + "'" + csvRecord.get(9) + "', '"          // User city (TEXT)
                    + csvRecord.get(10) + "', '"             // Result state (TEXT)
                    + csvRecord.get(11) + "')");           // Result country (TEXT)
                ps.execute();
            }

            ps.close();
            rs.close();
            con.close();  
            
            // // sleep to ensure that concurrent calls obtain separate Lambdas
            // try
            // {
            //     Thread.sleep(200);
            // }
            // catch (InterruptedException ie)
            // {
            //     logger.log("interrupted while sleeping...");
            // }
        }
        catch (Exception e) 
        {
            logger.log("Got an exception working with MySQL! ");
            System.out.println("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
            System.out.println(e.getMessage());
        }

        try {
            dataParser.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Delete S3 file.
        s3Client.deleteObject(new DeleteObjectRequest(bucketname, filename));

        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
