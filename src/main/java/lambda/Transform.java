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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import saaf.Inspector;

/**
 * Transform lambda function as part of the Transform-Load-Query pipeline for TCSS-462.
 * This lambda function is automatically invoked via a Cloud-Watch event when a file is placed in the correct S3 bucket.
 * This function transforms the file given by the S3 bucket event.
 *
 * @author Brandon Ragghianti
 * @author Michael
 * @author Tyler
 * @author Gabriel Stupart
 * @version 1.0
 */
public class Transform implements RequestHandler<HashMap<String, Object>,
                                                 HashMap<String, Object>> {

    /** API key for the OpenGate API */
    private static final String API_KEY = "";

    /** The S3 bucket for the transformed CSV file to be put. */
    private static final String PUT_BUCKET = "load.tlq";

    /** The S3 bucket for the recurring cities serialized object. */
    private static final String RECURRING_CITIES_BUCKET = "recurring-cities.tlq";

    /** The file name of the recurring cities serialized object. */
    private static final String RECURRING_CITIES_FILENAME = "recurring-cities";

    /**
     * Handler for the AWS lambda function. Automatically triggered by a Cloud-Watch event.
     * @param request The generated request from AWS. Must include a bucketname and filename property for the csv to be transformed.
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

        LambdaLogger logger = context.getLogger();

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
            dataParser = CSVParser.parse(objectData, Charset.defaultCharset(), CSVFormat.DEFAULT);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // Caches already queried cities.
        final Map<String, CacheLocation> recurringCities;
        if (s3Client.doesObjectExist(RECURRING_CITIES_BUCKET, RECURRING_CITIES_FILENAME)) {
            final InputStream serializedRecurringCities = s3Client.getObject(new GetObjectRequest(RECURRING_CITIES_BUCKET, RECURRING_CITIES_FILENAME)).getObjectContent();
            try {
                final ObjectInputStream serializedRecurringCitiesObjectStream = new ObjectInputStream(serializedRecurringCities);
                recurringCities = (HashMap<String, CacheLocation>) serializedRecurringCitiesObjectStream.readObject();
                serializedRecurringCitiesObjectStream.close();
            } catch (final IOException | ClassNotFoundException e) {
                logger.log("Could not deserialize recurring cities: " + e.getMessage());
                throw new RuntimeException(e);
            }
            try {
                serializedRecurringCities.close();
            } catch (final IOException e) {
                logger.log("Could not close retrieved S3 object: " + e.getMessage());
                throw new RuntimeException(e);
            }
        } else {
            recurringCities = new HashMap<>();
        }

        // Buffered writer for writing to /tmp on Lambda instance.
        final String tmpFileName = String.format("/tmp/%s", filename);
        final BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(tmpFileName));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // Iterate over all rows in the given CSV file.
        for (final CSVRecord record : dataParser) {

            // Transform row.
            final String transformedRow = transformRow(record, recurringCities);

            // Write completed String out to /tmp.
            try {
                writer.write(transformedRow);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            dataParser.close();
            writer.close();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // Delete S3 file.
        s3Client.deleteObject(new DeleteObjectRequest(bucketname, filename));

        // Load tmpFile.
        final File tmpFile = new File(tmpFileName);

        // Place tmpFile into next bucket.
        s3Client.putObject(PUT_BUCKET, filename, tmpFile);

        // Delete tmpFile from /tmp.
        if (!tmpFile.delete()) {
            System.err.println("Failed to delete temporary file: " + tmpFileName);
        }

        final String recurringCitiesFileLocation = String.format("/tmp/%s", RECURRING_CITIES_FILENAME);
        try {
            final FileOutputStream recurringCitiesOutput = new FileOutputStream(recurringCitiesFileLocation);
            final ObjectOutputStream recurringCitiesOutputStream = new ObjectOutputStream(recurringCitiesOutput);
            recurringCitiesOutputStream.writeObject(recurringCities);
            recurringCitiesOutputStream.close();
            recurringCitiesOutput.close();
        } catch (final IOException e) {
            logger.log("Could not properly write recurring cities: " + e.getMessage());
            throw new RuntimeException(e);
        }

        final File recurringCitiesFile = new File(recurringCitiesFileLocation);

        s3Client.putObject(RECURRING_CITIES_BUCKET, RECURRING_CITIES_FILENAME, recurringCitiesFile);

        // Delete tmpFile from /tmp.
        if (!recurringCitiesFile.delete()) {
            System.err.println("Failed to delete temporary file: " + tmpFileName);
        }

        //****************END FUNCTION IMPLEMENTATION***************************

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private static String transformRow(
            final CSVRecord record,
            final Map<String, CacheLocation> recurringCities
    ) {

        // Retrieve all data from the CSV row.
        final int userAge = Integer.parseInt(record.get(0));
        final String userGender = record.get(1);
        final int userNumberOfApps = Integer.parseInt(record.get(2));
        final float userSocialMediaUsage = Float.parseFloat(record.get(3));
        final float userProductivityAppUsage = Float.parseFloat(record.get(4));
        final float userGamingAppUsage = Float.parseFloat(record.get(5));
        final String userCity  = record.get(6);

        // Transform rows.
        final float userTotalAppUsage = userSocialMediaUsage + userProductivityAppUsage + userGamingAppUsage;
        final float userPercentOfSocialMedia = userSocialMediaUsage / userTotalAppUsage;
        final float userPercentOfProductivityAppUsage = userProductivityAppUsage / userTotalAppUsage;
        final float userPercentOfGamingAppUsage = userGamingAppUsage / userTotalAppUsage;

        // Find the state and country of the given city using the OpenCage API.
        String resultState = "N/A";
        String resultCountry = "N/A";

        // Check cache for current row city.
        if (recurringCities.containsKey(userCity)) {
            resultState = recurringCities.get(userCity).getState();
            resultCountry = recurringCities.get(userCity).getCountry();
        } else {
            try {

                // Create URL and query OpenCage API for given row city.
                final URL url = new URL(String.format("https://api.opencagedata.com/geocode/v1/json?q=%s&key=%s&limit=1", userCity.replace(" ", "%20"), API_KEY));
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.connect();
                if (conn.getResponseCode() == HttpStatus.SC_OK) {
                    final StringBuilder inline = new StringBuilder();
                    final Scanner jsonScanner = new Scanner(url.openStream());

                    // Put retrieved JSON into string and create JSONObject from string.
                    while (jsonScanner.hasNext()) {
                        inline.append(jsonScanner.nextLine());
                    }
                    final JSONObject jsonObject = new JSONObject(inline.toString());

                    // Retrieve the required section from the JSON to get the state and country.
                    final JSONObject data = jsonObject.getJSONArray("results").getJSONObject(0).getJSONObject("components");

                    // Retrieve state and country.
                    final String state = "state";
                    final String country = "country";
                    if (data.has(state)) {
                        resultState = data.getString(state);
                    }
                    if (data.has(country)) {
                        resultCountry = data.getString(country);
                    }

                    // Input queried city and retrieved state and country into cache.

                } else if (conn.getResponseCode() == HttpStatus.SC_UNAUTHORIZED) {
                    System.err.println("Invalid API Key");
                } else if (conn.getResponseCode() == HttpStatus.SC_BAD_REQUEST) {
                    System.err.println("Invalid API Request");
                } else if (conn.getResponseCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                    System.err.println("API Internal Server Error");
                } else if (conn.getResponseCode() == HttpStatus.SC_FORBIDDEN) {
                    System.err.println("API Quota Exceeded");
                } else {
                    System.err.println("Other API Error");
                }
            } catch (final IOException e) {
                System.err.println(e.getMessage());
            } finally {
                recurringCities.put(userCity, new CacheLocation(resultState, resultCountry));
            }
        }

        // All necessary data has been retrieved, build completed String.
        return String.format("%s,%s,%s,%s,%.2f,%s,%.2f,%s,%.2f,%.2f,%s,%s,%s\n", userAge, userGender, userNumberOfApps, userSocialMediaUsage, userPercentOfSocialMedia, userProductivityAppUsage, userPercentOfProductivityAppUsage, userGamingAppUsage, userPercentOfGamingAppUsage, userTotalAppUsage, userCity, resultState, resultCountry);
    }

    private static class CacheLocation implements Serializable {

        private static final long serialVersionUID = 3624983666763181265L;

        /** The State for the cached city. */
        private final String state;

        /** The Country for the cached city. */
        private final String country;

        CacheLocation(final String state, final String country) {
            this.state = state;
            this.country = country;
        }

        public String getState() {
            return state;
        }

        public String getCountry() {
            return country;
        }
    }
}

