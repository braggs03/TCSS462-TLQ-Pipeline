package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import saaf.Inspector;

public class Load implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    private static final String DATABASE_KEY_NAME = "MyDatabase";
    private static final String DATABASE_BUCKET_NAME = "TLQ_PIPELINE_TCSS462_GROUP5";
    private static final String DATABASE_LOCATION = "/tmp/mydata.db";

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
        
        String pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);

        logger.log("set pwd to tmp");        
        setCurrentDirectory("/tmp");
        
        pwd = System.getProperty("user.dir");
        logger.log("pwd=" + pwd);

        //****************START FUNCTION IMPLEMENTATION*************************

        // Retrieve the bucketname and filename from the S3 event JSON.
        final HashMap<?, ?> requestParameters = (HashMap<?, ?>) ((HashMap<?, ?>) request.get("detail")).get("requestParameters");
        final String bucketname = (String) requestParameters.get("bucketName");
        final String filename = (String) requestParameters.get("key");

        // Retrieve and the access the file from S3.
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        final S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        final InputStream objectData = s3Object.getObjectContent();

        // Create a CSVParser on the S3 file.
        final CSVParser dataParser;
        try {
            dataParser = CSVParser.parse(objectData, Charset.defaultCharset(), CSVFormat.DEFAULT.builder().setHeader().build());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // Delete S3 file.
        s3Client.deleteObject(new DeleteObjectRequest(bucketname, filename));
        
        if (s3Client.doesObjectExist(DATABASE_BUCKET_NAME, DATABASE_KEY_NAME)) {
            final S3Object s3Database = s3Client.getObject(new GetObjectRequest(bucketname, filename));
            final InputStream databaseData = s3Database.getObjectContent();
            try {
                FileUtils.copyInputStreamToFile(databaseData, new File(DATABASE_LOCATION));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try
        {
            // Connection string an in-memory SQLite DB
            // Connection con = DriverManager.getConnection("jdbc:sqlite:"); 

            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:/tmp/mydata.db");

            // Detect if the table 'mytable' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='mytable'");
            ResultSet rs = ps.executeQuery();
            if (!rs.next())
            {
                // 'mytable' does not exist, and should be created
                logger.log("trying to create table 'mytable'");
                ps = con.prepareStatement("CREATE TABLE mytable ( userID INTEGER PRIMARY KEY, userAge INTEGER, userGender TEXT, userNumberOfApps INTEGER, " + 
                "userSocialMediaUsage INTEGER, userPercentOfSocialMedia REAL, userProductivityAppUsage REAL, " +
                "userPercentOfProductivityAppUsage REAL, userGamingAppUsage REAL, userPercentOfGamingAppUsage REAL, " +
                "userCity TEXT, resultState TEXT, resultCountry TEXT);");
                ps.execute();
            }
            rs.close();

            for (CSVRecord csvRecord : dataParser) {    
                // Insert row into mytable (pattern of filled in variables done by ChatGPT)
                ps = con.prepareStatement("INSERT INTO mytable VALUES ("
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
        catch (SQLException sqle)
        {
            logger.log("DB ERROR:" + sqle.toString());
            sqle.printStackTrace();
        }
        
        s3Client.putObject(DATABASE_BUCKET_NAME, DATABASE_KEY_NAME, new File(DATABASE_LOCATION));
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    public static boolean setCurrentDirectory(String directory_name)
    {
        boolean result = false;  // Boolean indicating whether directory was set
        File    directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs())
        {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }
}
