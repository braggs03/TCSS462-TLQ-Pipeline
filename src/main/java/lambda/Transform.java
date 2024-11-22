package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import saaf.Inspector;

import org.json.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Transform implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************


        String bucketname = (String) ((HashMap)((HashMap)request.get("detail")).get("requestParameters")).get("bucketName");
        String filename = (String) ((HashMap)((HashMap)request.get("detail")).get("requestParameters")).get("key");

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        InputStream objectData = s3Object.getObjectContent();
        StringBuilder text = new StringBuilder();
        Scanner scanner = new Scanner(objectData);
        while (scanner.hasNext()) {
            text.append(scanner.nextLine()).append("\n");
        }
        scanner.close();

        CSVParser dataParser;
        try {
            dataParser = CSVParser.parse(text.toString(), CSVFormat.DEFAULT.builder().setHeader().build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> recurringCities = new HashMap<>();
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(String.format(".\\tmp\\%s", filename)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (CSVRecord record : dataParser) {
            int userAge = Integer.parseInt(record.get(0));
            String userGender = record.get(1);
            int userNumberOfApps = Integer.parseInt(record.get(2));
            float userSocialMediaUsage = Float.parseFloat(record.get(3));
            float userProductivityAppUsage = Float.parseFloat(record.get(4));
            float userGamingAppUsage = Float.parseFloat(record.get(5));
            String userCity  = record.get(6);

            float userTotalAppUsage = userSocialMediaUsage + userProductivityAppUsage + userGamingAppUsage;
            float userPercentOfSocialMedia = userSocialMediaUsage / userTotalAppUsage;
            float userPercentOfProductivityAppUsage = userProductivityAppUsage / userTotalAppUsage;
            float userPercentOfGamingAppUsage = userGamingAppUsage / userTotalAppUsage;

            String resultState = "N/A";
            if (recurringCities.containsKey(userCity)) {
                resultState = recurringCities.get(userCity);
            } else {

            }

            String transformedRow = String.format("%s,%s,%s,%s,%.2f,%s,%.2f,%s,%.2f,%s,%s\n", userAge, userGender, userNumberOfApps, userSocialMediaUsage, userPercentOfSocialMedia, userProductivityAppUsage, userPercentOfProductivityAppUsage, userGamingAppUsage, userPercentOfGamingAppUsage, userCity, resultState);

            try {
                writer.append(transformedRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}

