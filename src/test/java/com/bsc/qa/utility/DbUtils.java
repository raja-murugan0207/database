package com.bsc.qa.utility;

import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public class DbUtils {

	private Connection facetsConnection = null;
	private Connection wprConnection = null;
	private Connection connection = null;
	private Connection msSqlConnection = null;

	// Setup the database connection using the oracle JDBC driver

	public Connection setUp(String connectionType) {
		Connection connection = null;
		String database = null;
		String user = null;
		String password = null;
		String server = null;
		String port = null;
		String connectionString = null;

		if ("facets".equals(connectionType)) {
			database = System.getenv("FACETS_DB");
			user = System.getenv("FACETS_USER");
			password = System.getenv("FACETS_PASSWORD");
			server = System.getenv("FACETS_SERVER");
			port = System.getenv("FACETS_PORT");
			connectionString = "jdbc:oracle:thin:" + user.trim() + "/" + password.trim() + "@" + server.trim() + ":"
					+ port.trim() + ":" + database.trim();
		} else if ("wpr".equals(connectionType)) {
			database = System.getenv("WPR_DB");
			user = System.getenv("WPR_USER");
			password = System.getenv("WPR_PASSWORD");
			server = System.getenv("WPR_SERVER");
			port = System.getenv("WPR_PORT");
			connectionString = "jdbc:oracle:thin:" + user.trim() + "/" + password.trim() + "@" + server.trim() + ":"
					+ port.trim() + ":" + database.trim();
		} else if ("mssql".equals(connectionType)) {
			database = System.getenv("EDIFECS_MSSQL_DB");
			user = System.getenv("EDIFECS_MSSQL_USER");
			password = System.getenv("EDIFECS_MSSQL_PASSWORD");
			server = System.getenv("EDIFECS_MSSQL_SERVER");
			port = System.getenv("EDIFECS_MSSQL_PORT");
			connectionString = "jdbc:sqlserver://" + server + ":" + port + ";DatabaseName=" + database + ";user=" + user
					+ ";password=" + password;
		}

		try {
			connection = DriverManager.getConnection(connectionString);
		} catch (SQLException ex) {
			System.out.println("ERROR: SQL Exception when connecting to the database: " + database);

		}

		return connection;

	}

	// Close the database connection

	public void tearDown() {
		if (facetsConnection != null) {
			try {
				// System.out.println("Closing Facets Database Connection...");
				facetsConnection.close();
				facetsConnection = null;
			} catch (SQLException ex) {
				System.out.println("SQLException" + ex);
			}
		}
		if (wprConnection != null) {
			try {
				// System.out.println("Closing WPR Database Connection...");
				wprConnection.close();
				wprConnection = null;
			} catch (SQLException ex) {
				System.out.println("SQLException" + ex);
			}
		}
		if (connection != null) {
			try {
				// System.out.println("Closing WPR Database Connection...");
				connection.close();
				connection = null;
			} catch (SQLException ex) {
				System.out.println("SQLException" + ex);
			}
		}
		if (msSqlConnection != null) {
			try {
				// System.out.println("Closing WPR Database Connection...");
				msSqlConnection.close();
				msSqlConnection = null;
			} catch (SQLException ex) {
				System.out.println("SQLException" + ex);
			}
		}
	}

	// Prepared prepared statement
	// sql Prepared query
	// values Values for the prepared query
	// return PreparedStatement
	// throws SQLException

	private PreparedStatement preparePreparedStatement(String sql, Object... values) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
		for (int i = 0; i < values.length; i++) {
			preparedStatement.setObject(i + 1, values[i]);
		}
		return preparedStatement;
	}

	public Multimap<String, String> getdatadumpquery(String dbSource, String queryinput) throws IOException {

		connection = setUp(dbSource);
		String value = "";
		String columnName = "";
		PreparedStatement preparedStatement;
		ResultSet resultSet;
		Multimap<String, String> data = LinkedListMultimap.create();
		try {

			preparedStatement = preparePreparedStatement(queryinput);
			resultSet = preparedStatement.executeQuery();
			ResultSetMetaData rsmd = resultSet.getMetaData();
			for (int rowcount = 0; rowcount <= resultSet.getRow(); rowcount++) {
				if (resultSet.next()) {
					for (int i = 0; i < rsmd.getColumnCount(); i++) {
						columnName = rsmd.getColumnName(i + 1);
						value = resultSet.getString(columnName);
						data.put(columnName, value);
					}

				}

			}
		} catch (SQLException ex) {
			System.out.println("exception occured " + ex);

		}

		tearDown();

		return data;

	}

}
