package com.bsc.qa.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import com.bsc.qa.framework.utility.ExcelUtils;
import com.bsc.qa.utility.CsvUtils;
import com.bsc.qa.utility.DbUtils;
import com.google.common.collect.Multimap;

public class BackEndDataExtractMain {

	public static void main(String[] args) {
		// Get the arguments vales or path to return the method.
		String inputfilepath = "";
		String outputfilepath = "";
		for (int i = 0; i <= args.length; i++) {
			if (!(args.length == 0)) {
				inputfilepath = args[0];
				outputfilepath = args[1];

			}
			dataBaseQueryExecution(inputfilepath, outputfilepath);
		}

		// To execute the database query execution
		// generate the CSV output file corresponding output file.

	}

	// Get the values and return the queries.
	// Return the data to write the CSV files.
	public static void dataBaseQueryExecution(String inputfilepath, String outputfilepath) {
		String rowqueryvalue = "";
		String rowreportvalue = "";
		String dbnameinput = "";
		File folder;
		DbUtils dataBase = new DbUtils();
		String name = null;
		String[] header = null;
		// Excel input Environment variable folder name
		folder = new File(inputfilepath);
		// Arguments file path is empty to fetch the default NAS folder excel to execute
		// the queries

		if (inputfilepath == "") {
			folder = new File(System.getenv("DATAEXTRACTIONUTILITY_INPUT"));

		}
		try {
			File[] listOfFiles;
			// get the no of excel file in corresponding folder
			listOfFiles = folder.listFiles();
			for (File file : listOfFiles) {
				if (file.isFile()) {
					String excelPath = file.getAbsolutePath();

					// Iterating the excel workbook.
					Object[][] exceldata = ExcelUtils.getTableArray(excelPath, "InputQueries");

					// Iterating the each column by column
					for (Object[] excelvalues : exceldata) {
						// fetching the column value row by row in the excel sheet
						rowqueryvalue = excelvalues[0].toString();
						rowreportvalue = excelvalues[1].toString();
						dbnameinput = excelvalues[2].toString();
						LinkedHashMap<String, String> mapQueryOutput = new LinkedHashMap<String, String>();
						Multimap<String, String> multivaluequeryoutput = null;
						// based on input queries to execute the switch cases
						switch (dbnameinput) {
						// Get the facets queries to return the data base execution
						case "FACETS":
							multivaluequeryoutput = dataBase.getdatadumpquery("facets", rowqueryvalue);

							break;
						// Get the mssql queries to return the data base execution
						case "EDIFECS":

							multivaluequeryoutput = dataBase.getdatadumpquery("mssql", rowqueryvalue);
							break;
						// Get the wpr queries to return the data base execution
						case "WPR":
							multivaluequeryoutput = dataBase.getdatadumpquery("wpr", rowqueryvalue);
							break;
						default:
							break;

						}

						if (!(multivaluequeryoutput.size() == 0)) {
							List<String[]> csvDataList = new ArrayList<String[]>();
							// Get the column count
							int columncount = multivaluequeryoutput.keySet().size();
							// get the column with row counts
							int multimap = multivaluequeryoutput.values().size();

							// get exact row count

							int rowcount = multimap / columncount;

							Collection<String> getvalues = multivaluequeryoutput.values();
							// Iterating the each column value
							Iterator<String> iteraterow = getvalues.iterator();
							for (int i = 0; i < rowcount; i++) {
								for (String primarykey : multivaluequeryoutput.keySet()) {
									if (iteraterow.hasNext()) {
										name = iteraterow.next();
									}
									// Adding the single key and multiple values
									mapQueryOutput.put(primarykey, name);

								}

								// Write the Header in CSV Report
								header = multivaluequeryoutput.keySet()
										.toArray(new String[multivaluequeryoutput.size()]);
								// write the column values
								String[] dataSet = mapQueryOutput.values().toArray(new String[mapQueryOutput.size()]);
								// Add the all column values
								csvDataList.add(dataSet);

							}
							CsvUtils.writeAllData(csvDataList, header, rowreportvalue, outputfilepath);
						}

					}

				}

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception while writing to data" + e.getMessage());
		}

	}

}