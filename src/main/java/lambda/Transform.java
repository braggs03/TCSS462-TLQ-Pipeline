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

import bridges.connect.Bridges;
import bridges.connect.DataSource;
import bridges.data_src_dependent.City;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Transform implements RequestHandler<Request, HashMap<String, Object>> {

    public HashMap<String, Object> handleRequest(Request request, Context context) {

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************

        String bucketname = request.getBucketname();
        String filename = request.getFilename();

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

        Bridges bridges = new Bridges(1, "braggs03", "892460948341");
        DataSource ds = bridges.getDataSource();

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
                HashMap<String, String> map = new HashMap<>();
                map.put("city", userCity.replace(" ", "%20"));
                List<City> cities = null;
                try {
                    cities = ds.getUSCitiesData(map);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                cities.sort((o1, o2) -> o2.getPopulation() - o1.getPopulation());
                if (!cities.isEmpty()) {
                    String userState = cities.get(0).getState();
                    resultState = userState;
                    recurringCities.put(userCity, userState);
                } else {
                    recurringCities.put(userCity, "N/A");
                }
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

    public static void main(String[] args) throws IOException {


        File objectData = new File("mobile.csv");
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

        String filename = objectData.getName();

        Bridges bridges = new Bridges(1, "braggs03", "892460948341");
        DataSource ds = bridges.getDataSource();

        Map<String, String> recurringCities = new HashMap<>();
        BufferedWriter writer = new BufferedWriter(new FileWriter(String.format(".\\tmp\\%s", filename)));
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
                HashMap<String, String> map = new HashMap<>();
                map.put("city", userCity.replace(" ", "%20"));
                List<City> cities = ds.getUSCitiesData(map);
                cities.sort((o1, o2) -> o2.getPopulation() - o1.getPopulation());
                if (!cities.isEmpty()) {
                    String userState = cities.get(0).getState();
                    resultState = userState;
                    recurringCities.put(userCity, userState);
                } else {
                    recurringCities.put(userCity, "N/A");
                }
            }

            String transformedRow = String.format("%s,%s,%s,%s,%.2f,%s,%.2f,%s,%.2f,%s,%s\n", userAge, userGender, userNumberOfApps, userSocialMediaUsage, userPercentOfSocialMedia, userProductivityAppUsage, userPercentOfProductivityAppUsage, userGamingAppUsage, userPercentOfGamingAppUsage, userCity, resultState);

            writer.append(transformedRow);
        }
        writer.close();
    }
}

