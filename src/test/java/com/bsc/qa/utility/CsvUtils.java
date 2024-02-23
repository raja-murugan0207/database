package com.bsc.qa.utility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import au.com.bytecode.opencsv.CSVWriter;

//CsvUtils class is used for CSV writing

public class CsvUtils {

	// to store the time stamp
	public static String timestamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());

	// To generate the CSV file with corresponding value.
	public static void writeAllData(List<String[]> data, String[] header, String reportName, String outputfilepath) {

		try {
			String outputFile;
			if (outputfilepath == "") {
				outputFile = System.getenv("DATAEXTRACTIONUTILITY_OUTPUT") + "\\" + reportName + "_" + timestamp
						+ ".csv";
				;
			} else {
				outputFile = outputfilepath + "\\" + reportName + "_" + timestamp + ".csv";
			}
			CSVWriter writer = new CSVWriter(Files.newBufferedWriter(Paths.get(outputFile)), ',');
			// Write the header in the CSV file
			writer.writeNext(header);
			// Write the values in CSV file
			writer.writeAll(data);
			// Close the CSV file.
			writer.close();
		} catch (IOException e) {
			System.out.println("Exception while writing to csv" + e.getMessage());
		}
	}

}
