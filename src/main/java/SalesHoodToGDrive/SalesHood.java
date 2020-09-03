package SalesHoodToGDrive;

import org.json.CDL;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class SalesHood {

    public static String getNewAPI(String type, String token) throws Exception {
        String url = "https://api.saleshood.com/api/v1/get_datasets?type=" + type;
        JSONArray output = getNewResult(url,token);

        try {
            String csv = CDL.toString(output);
            return csv;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getNewResult(String urlToRead, String token) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlToRead);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Token",token);
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();

        JSONArray output = new JSONArray(result.toString());

        return output;
    }

}
