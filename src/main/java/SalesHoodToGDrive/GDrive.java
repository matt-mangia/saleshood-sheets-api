package SalesHoodToGDrive;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.*;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.*;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class GDrive {

    private static final String APPLICATION_NAME = "SalesHood-Sheets-Upload";

    public static void updateSheet(String gDriveAPICred, String spreadsheetId, String sheetname, String dataToUpload) throws Exception {

        BufferedReader bufReader = new BufferedReader(new StringReader(dataToUpload));
        Sheets sheetsService = createSheetsService(gDriveAPICred);

        checkExistsAndClear(sheetsService,spreadsheetId, sheetname);

        HashMap<String,Integer> sheetMap = getSheetMap(sheetsService,spreadsheetId);

        // Set range to write into.
        String range = "'"+sheetname+"'!A1";
        // How the input data should be interpreted.
        String valueInputOption = "RAW";
        // How the input data should be inserted.
        String insertDataOption = "OVERWRITE";

        // Format data into nested list to be inserted via append
        List<List<Object>> data = new ArrayList<List<Object>>();

        if (dataToUpload != null) {
            String line = bufReader.readLine();
            while( line != null )
            {
                data.add(Arrays.asList(line));
                line = bufReader.readLine();
            }
        }

        // Assign values to be inserted with append
        ValueRange requestBody = new ValueRange();
        requestBody.setValues(data);

        Sheets.Spreadsheets.Values.Append request =
                sheetsService.spreadsheets().values().append(spreadsheetId, range, requestBody);
        request.setValueInputOption(valueInputOption);
        request.setInsertDataOption(insertDataOption);

        AppendValuesResponse response = request.execute();

        // TODO: Change code below to process the `response` object:
        System.out.println(response);

        textToColumns(sheetsService, spreadsheetId, sheetMap.get(sheetname),",");
    }

    public static void formatSheets(String gDriveAPICred, String spreadsheetId) throws Exception {
        Sheets sheetsService = createSheetsService(gDriveAPICred);
        HashMap<String,Integer> sheetMap = getSheetMap(sheetsService, spreadsheetId);

        // A list of updates to apply to the spreadsheet.
        // Requests will be applied in the order they are specified.
        // If any request is not valid, no requests will be applied.
        List<Request> requests = new ArrayList<>();

        // Format Huddles sheet
        Integer sheetId = sheetMap.get("huddle_event");
        DimensionRange source = new DimensionRange().setSheetId(sheetId).setDimension("COLUMNS").setStartIndex(5).setEndIndex(6);
        requests.add(new Request().setMoveDimension(new MoveDimensionRequest().setSource(source).setDestinationIndex(0)));

        // format user sheet
        sheetId = sheetMap.get("user");
        source = new DimensionRange().setSheetId(sheetId).setDimension("COLUMNS").setStartIndex(10).setEndIndex(11);
        requests.add(new Request().setMoveDimension(new MoveDimensionRequest().setSource(source).setDestinationIndex(0)));

        // format learning path sheet
        sheetId = sheetMap.get("learning_path");
        source = new DimensionRange().setSheetId(sheetId).setDimension("COLUMNS").setStartIndex(6).setEndIndex(7);
        requests.add(new Request().setMoveDimension(new MoveDimensionRequest().setSource(source).setDestinationIndex(0)));

        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
        requestBody.setRequests(requests);

        Sheets.Spreadsheets.BatchUpdate request =
                sheetsService.spreadsheets().batchUpdate(spreadsheetId, requestBody);
        BatchUpdateSpreadsheetResponse response = request.execute();

        System.out.println(response);

        createHuddleCompletionSheet(sheetsService,spreadsheetId);
    }

    private static Sheets createSheetsService(String credFileName) throws IOException, GeneralSecurityException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        // authorization
        GoogleCredentials credentials = authorize(credFileName);
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);

        // set up the global Drive instance
        return new Sheets.Builder(httpTransport, jsonFactory, requestInitializer).setApplicationName(
                APPLICATION_NAME).build();
    }

    /** Authorizes the installed application to access user's protected data. */
    private static GoogleCredentials authorize(String credFileName) throws IOException {
        java.io.File credFile = new java.io.File(credFileName);
        InputStream credStream = new FileInputStream(credFile);
        GoogleCredentials credentials = ServiceAccountCredentials
                .fromStream(credStream)
                .createScoped(Arrays.asList("https://www.googleapis.com/auth/drive"));
        return credentials;
    }

    private static void textToColumns(Sheets sheetsService, String spreadsheetId, Integer sheetId, String delimiter) throws IOException, GeneralSecurityException {

        GridRange source;
        source = new GridRange().setSheetId(sheetId).setStartColumnIndex(0).setEndColumnIndex(1);

        String delimiterType = "COMMA";
        if (!delimiter.equalsIgnoreCase(",")){
            delimiterType = "CUSTOM";
        }

        // A list of updates to apply to the spreadsheet.
        // Requests will be applied in the order they are specified.
        // If any request is not valid, no requests will be applied.
        List<Request> requests = new ArrayList<>();
        requests.add(new Request().setTextToColumns(new TextToColumnsRequest().setDelimiterType(delimiterType).setDelimiter(delimiter).setSource(source)));

        BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
        requestBody.setRequests(requests);

        Sheets.Spreadsheets.BatchUpdate request =
                sheetsService.spreadsheets().batchUpdate(spreadsheetId, requestBody);
        BatchUpdateSpreadsheetResponse response = request.execute();

        System.out.println(response);
    }

    private static void checkExistsAndClear (Sheets sheetsService, String spreadsheetId, String sheetname) throws Exception {
        // get sheet name > id mapping
        HashMap<String, Integer> sheetMap = getSheetMap(sheetsService,spreadsheetId);

        // check sheet exists. If not, create, ELSE clear existing sheet
        if (!sheetMap.containsKey(sheetname)){
            List<Request> requests = new ArrayList<>();
            requests.add(new Request().setAddSheet(new AddSheetRequest().setProperties(new SheetProperties().setTitle(sheetname).setHidden(true))));

            BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
            requestBody.setRequests(requests);

            Sheets.Spreadsheets.BatchUpdate request =
                    sheetsService.spreadsheets().batchUpdate(spreadsheetId, requestBody);

            BatchUpdateSpreadsheetResponse response = request.execute();
            System.out.println(response);
        }
        else {
            // Clear Sheet to be written into
            String clearRange = "'"+sheetname+"'";
            ClearValuesRequest clearRequestBody = new ClearValuesRequest();

            Sheets.Spreadsheets.Values.Clear clearRequest =
                    sheetsService.spreadsheets().values().clear(spreadsheetId, clearRange, clearRequestBody);
            ClearValuesResponse clearResponse = clearRequest.execute();
            System.out.println(clearResponse);
        }
    }

    private static HashMap<String,Integer> getSheetMap(Sheets sheetsService, String spreadsheetId) throws Exception{
        Spreadsheet response = sheetsService.spreadsheets().get(spreadsheetId).setIncludeGridData(false).execute();
        List<Sheet> workSheetList = response.getSheets();

        HashMap<String, Integer> sheetMap = new HashMap<>();

        for (Sheet sheet : workSheetList) {
            sheetMap.put(sheet.getProperties().getTitle(), sheet.getProperties().getSheetId());
        }
        return sheetMap;
    }

    private static HashMap<String,Integer> getSheetRowCounts(Sheets sheetsService, String spreadsheetId) throws Exception{
        Spreadsheet response = sheetsService.spreadsheets().get(spreadsheetId).setIncludeGridData(false).execute();
        List<Sheet> workSheetList = response.getSheets();

        HashMap<String, Integer> sheetMap = new HashMap<>();

        for (Sheet sheet : workSheetList) {
            sheetMap.put(sheet.getProperties().getTitle(), sheet.getProperties().getGridProperties().getRowCount());
        }
        return sheetMap;
    }

    public static void createHuddleCompletionSheet(Sheets sheetsService, String spreadsheetId) throws Exception{
        String sheetname  = "Raw Huddle Completion Data";

        checkExistsAndClear(sheetsService,spreadsheetId,sheetname);

        // Set range to write into.
        String range = "'"+sheetname+"'!A1";
        // How the input data should be interpreted.
        String valueInputOption = "USER_ENTERED";
        // How the input data should be inserted.
        String insertDataOption = "OVERWRITE";

        // Format data into nested list to be inserted via append
        List<List<Object>> data = new ArrayList<List<Object>>();

        data.add(Arrays.asList(new String[]{"huddle_event", "huddle_name", "user", "user_name", "user_mgr","user_segment","VlookupKey","huddle_comp_pct","completion_date"}));

        data.add(Arrays.asList(new String[]{"='huddle_event_participation_fact'!$A2",
                                            "=VLOOKUP($A2,'huddle_event'!$A:$F,5)",
                                            "='huddle_event_participation_fact'!$H2",
                                            "=VLOOKUP($C2,'user'!$A:$H,5,FALSE)",
                                            "=VLOOKUP($C2,'user'!$A:$H,2,FALSE)",
                                            "=VLOOKUP($C2,'user'!$A:$H,6,FALSE)",
                                            "=CONCATENATE($A2,\"-\",$D2)",
                                            "='huddle_event_participation_fact'!$D2",
                                            "='huddle_event_participation_fact'!$F2"}));

        // Assign values to be inserted with append
        ValueRange requestBody = new ValueRange();
        requestBody.setValues(data);
        requestBody.setMajorDimension("ROWS");

        Sheets.Spreadsheets.Values.Append request =
                sheetsService.spreadsheets().values().append(spreadsheetId, range, requestBody);
        request.setValueInputOption(valueInputOption);
        request.setInsertDataOption(insertDataOption);

        AppendValuesResponse response = request.execute();

        System.out.println(response);


        HashMap<String,Integer> sheetMap = getSheetMap(sheetsService, spreadsheetId);
        HashMap<String,Integer> sheetRowCounts = getSheetRowCounts(sheetsService, spreadsheetId);


        // A list of updates to apply to the spreadsheet.
        // Requests will be applied in the order they are specified.
        // If any request is not valid, no requests will be applied.
        List<Request> requests = new ArrayList<>();

        Integer sheetId = sheetMap.get(sheetname);
        Integer rowCount = sheetRowCounts.get("huddle_event_participation_fact");

        DimensionRange newRows = new DimensionRange().setDimension("ROWS").setSheetId(sheetId).setEndIndex(rowCount).setStartIndex(2);
        requests.add(new Request().setInsertDimension(new InsertDimensionRequest().setRange(newRows)));

        GridRange source = new GridRange().setSheetId(sheetId).setStartRowIndex(1).setEndRowIndex(2);
        GridRange destination = new GridRange().setSheetId(sheetId).setStartRowIndex(2).setEndRowIndex(rowCount);
        requests.add(new Request().setCopyPaste(new CopyPasteRequest().setSource(source).setDestination(destination)));

        BatchUpdateSpreadsheetRequest copyrequestbody = new BatchUpdateSpreadsheetRequest();
        copyrequestbody.setRequests(requests);

        Sheets.Spreadsheets.BatchUpdate copyrequest =
                sheetsService.spreadsheets().batchUpdate(spreadsheetId, copyrequestbody);
        BatchUpdateSpreadsheetResponse copyresponse = copyrequest.execute();

        System.out.println(copyresponse);




    }

}
