package SalesHoodToGDrive;

import java.util.*;
import org.apache.commons.cli.*;


public class SalesHoodToGDrive {
    private static String token;
    private static String gDriveApiCred;
    private static String gDriveSpreadsheetId;

    public static void main(String[] args) throws Exception {

        CommandLine commandLine;
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addRequiredOption("t","token",true,"SalesHood API Token String");
        options.addRequiredOption("g","gDriveApiCred",true,"Google Drive API Service Acct Credential File Location");
        options.addRequiredOption("s","spreadsheetId", true,"Destination Google Drive SpreadsheetId");
        try {
            commandLine = parser.parse(options, args);
            token = commandLine.getOptionValue("t");
            gDriveApiCred = commandLine.getOptionValue("g");
            gDriveSpreadsheetId = commandLine.getOptionValue("s");
        } catch (ParseException exception) {
            System.err.println("Argument Error: " + exception.getMessage());
            System.exit(1);
        }

        List<String> salesHoodDatasets = new ArrayList<>();
        salesHoodDatasets.add("huddle_event_participation_fact");
        salesHoodDatasets.add("user");
        salesHoodDatasets.add("huddle_event");
        salesHoodDatasets.add("learning_path_participation_fact");
        salesHoodDatasets.add("learning_path");

        for (String api : salesHoodDatasets){
            String data = SalesHood.getNewAPI(api,token);
            GDrive.updateSheet(gDriveApiCred,
                    gDriveSpreadsheetId,
                    api,
                    data);
        }
        GDrive.formatSheets(gDriveApiCred, gDriveSpreadsheetId);
    }
}