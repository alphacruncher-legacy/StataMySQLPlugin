/**
 * Copyright 2015 by Alphacruncher AG
 */
package com.alphacruncher;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.stata.sfi.Data;
import com.stata.sfi.SFIToolkit;

/**
 * @author Daniel Sali (daniel.sali@alphacruncher.com)
 * 
 * MySQL connection plugin for Stata. The plugin is able to
 * execute SELECT queries on MySQL database tables. The plugin's query function
 * creates variables corresponding to the columns of the result table
 * and stores the result rows as observations.
 * 
 * To use the plugin, a JDBC URL needs to be specified, such as:
 * <tt>jdbc:mysql://localhost:3306/db</tt>
 * 
 * Also, a path to the connection properties file needs to be specified, which contains the username and password:
 * <tt>
 * user=TestUser
 * password=Password123
 * </tt>
 * Usage example (Stata commands):
 * <tt>
 * javacall com.alphacruncher.StataMySQLPlugin initialize, args(<JDBC URL>, <path_to_connection_file>)
 * javacall com.alphacruncher.StataMySQLPlugin query, args("SELECT * FROM .. LIMIT 100")
 * print
 * </tt>
 * The <tt>initialize</tt> function only needs to be called once per session,
 * or when the user wishes to connect to a different database or connect as a different user.
 * 
 * Multiple <tt>query</tt> calls can be made in a session, the results of the latest query will be appended
 * to the existing observations. * 
 */
public class StataMySQLPlugin {
	private static String jdbcURL = null;
	private static Properties connProps = null;
	private static Connection conn = null;
	
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	private static final DateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSSSSS");
	private static final DateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
	private static final DateFormat TIME_TZ_FORMAT = new SimpleDateFormat("HH:mm:ss.SSSSSSZ");
	private static final DateFormat TIMESTAMP_TZ_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ");
	
	public static int help(String[] args) {
		SFIToolkit.displayln("Usage example:");
		SFIToolkit.displayln("javacall com.alphacruncher.StataMySQLPlugin initialize, args(<JDBC URL> <path_to_connection_file>)");
		SFIToolkit.displayln("javacall com.alphacruncher.StataMySQLPlugin query, args(\"SELECT * FROM .. LIMIT 100 \")");
		return 0;
	}

	public static int initialize(String[] args) {
		try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception e) {
        	SFIToolkit.errorln("Failed to initialize MySQL JDBC Driver: " + e.getMessage());
			return (198);
        }

		if (args.length != 2) {
			SFIToolkit.errorln("Please specify the JDBC URL and the path of the database connection properties file which contains the username and password.");
			return (198);
		}
		if (!args[0].startsWith("jdbc:mysql")) {
			SFIToolkit.errorln("The JDBC URL given is not valid, it should be like: jdbc:mysql://localhost:3306/db");
			return (198);
		}
		jdbcURL = args[0];
		
		connProps = new Properties();
		try {
			connProps.load(new BufferedReader(new FileReader(args[1])));
		} catch (FileNotFoundException e) {
			SFIToolkit.errorln("Database connection properties file could not be found: " + e.getMessage());
			return (198);
		} catch (IOException e) {
			SFIToolkit.errorln("Database connection properties file could not be read: " + e.getMessage());
			return (198);
		}
		SFIToolkit.displayln("MySQL plugin successfully initialized. User: " + connProps.getProperty("user"));
		return 0;
	}
	
	public static int query(String[] args) {
		if (null == connProps || null == jdbcURL) {
			SFIToolkit.errorln("Please initialize the DB connection first with:");
			SFIToolkit.errorln("javacall com.alphacruncher.StataMySQLPlugin initialize, args(<JDBC URL> <path_to_connection_file>)");
		}
		if (args.length != 1) {
			SFIToolkit.errorln("Please specify the SELECT query to execute.");
			return (198);
		}
		int initialObs = Data.getObsCount();
		PreparedStatement stmnt = null;
		ResultSet res = null;
		try {
			conn = DriverManager.getConnection(jdbcURL, connProps);
			SFIToolkit.displayln("Successfully connected to the database, running query...");
			
			stmnt = conn.prepareStatement(args[0]);
			res = stmnt.executeQuery();
			int columnCount = res.getMetaData().getColumnCount();
			List<Integer> columnTypes = new ArrayList<Integer>(columnCount);
			List<Integer> stataVarIndices = new ArrayList<Integer>(columnCount);
			
			// Checks if the Stata variables exists for the result columns, creates them if necessary.
			for (int i = 1; i <= columnCount; ++i) {
				columnTypes.add(res.getMetaData().getColumnType(i));
				String stataVarName = Data.makeVarName(res.getMetaData().getColumnLabel(i), true);
				createStataVar(res, i, stataVarName);
				stataVarIndices.add(Data.getVarIndex(stataVarName));
				
			}
			
			// Gets the result row count, inefficient, but no better way possible
			res.last();
			int rowCount = res.getRow();
			res.beforeFirst();
			SFIToolkit.displayln("Retrieved " + rowCount + " rows.");
			Data.setObsCount(rowCount + initialObs);
			SFIToolkit.displayln("Observation count set to: " + Data.getObsCount());
			
			int obs = 0;
			// Saves the result rows as observation values
			while (res.next()) {
				++obs;
				for (int i = 1; i <= columnCount; ++i) {
					saveRecordToDataset(initialObs, res, columnTypes,
							stataVarIndices, obs, i);
				}
			}
			
			SFIToolkit.displayln("Data loaded successfully into observations " + (initialObs + 1) + " to " + (initialObs + rowCount));
			return 0;
			
		} catch (SQLException e) {
			SFIToolkit.errorln("Failed to query database: " + e.getMessage());
			SFIToolkit.errorln("SQL State: " + e.getSQLState());
		    SFIToolkit.errorln("VendorError: " + e.getErrorCode());
		    return (198);
		} catch (Exception e) {
			SFIToolkit.error("Failed to query database: " + e.getMessage());
			return (198);
		} finally {
			 if (res != null) {
		        try {
		            res.close();
		        } catch (SQLException sqlEx) { } // ignore
		        res = null;
		    }
		    if (stmnt != null) {
		        try {
		            stmnt.close();
		        } catch (SQLException sqlEx) { } // ignore
		        stmnt = null;
		    }
		    if (conn != null) {
		    	try {
		    		conn.close();
		    	} catch (SQLException sqlEx) { } // ignore
		    	conn = null;
		    }
		}

	}

	private static void saveRecordToDataset(int initialObs, ResultSet res,
			List<Integer> columnTypes, List<Integer> stataVarIndices, int obs,
			int i) throws SQLException {
		switch (columnTypes.get(i-1)) {
		case java.sql.Types.BIGINT:
		case java.sql.Types.ROWID:
			Data.storeNum(stataVarIndices.get(i-1), initialObs + obs, res.getLong(i));
			break;
		case java.sql.Types.INTEGER:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.TINYINT:
			Data.storeNum(stataVarIndices.get(i-1), initialObs + obs, res.getInt(i));
			break;
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.BIT:
			Data.storeNum(stataVarIndices.get(i-1), initialObs + obs, res.getBoolean(i) ? 1 : 0);
			break;
		case java.sql.Types.DECIMAL:
		case java.sql.Types.NUMERIC:
			Data.storeNum(stataVarIndices.get(i-1), initialObs + obs, res.getBigDecimal(i).doubleValue());
			break;
		case java.sql.Types.DOUBLE:
		case java.sql.Types.FLOAT:
		case java.sql.Types.REAL:
			Data.storeNum(stataVarIndices.get(i-1), initialObs + obs, res.getDouble(i));
			break;
		case java.sql.Types.CHAR:
		case java.sql.Types.NCHAR:
		case java.sql.Types.NVARCHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.LONGVARCHAR:
		case java.sql.Types.LONGNVARCHAR:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, res.getString(i));
			break;
		case java.sql.Types.CLOB:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, res.getClob(i).toString());
			break;
		case java.sql.Types.NCLOB:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, res.getNClob(i).toString());
			break;
		case java.sql.Types.DATE:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, DATE_FORMAT.format(res.getDate(i)));
			break;
		case java.sql.Types.TIME:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, TIME_FORMAT.format(res.getTimestamp(i)));
			break;
		case java.sql.Types.TIME_WITH_TIMEZONE:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, TIME_TZ_FORMAT.format(res.getTime(i)));
			break;
		case java.sql.Types.TIMESTAMP:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, TIMESTAMP_FORMAT.format(res.getTimestamp(i)));
			break;
		case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
			Data.storeStr(stataVarIndices.get(i-1), initialObs + obs, TIMESTAMP_TZ_FORMAT.format(res.getTimestamp(i)));
			break;
		default:
			break;
		}
	}

	private static void createStataVar(ResultSet res, int i,
			String stataVarName) throws SQLException, Exception {
		switch (res.getMetaData().getColumnType(i)) {
		case java.sql.Types.BIGINT:
		case java.sql.Types.ROWID:
			Data.addVarLong(stataVarName);
			SFIToolkit.displayln("Added new Long variable '" + stataVarName + "' to dataset.");
			break;
		case java.sql.Types.INTEGER:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.TINYINT:
			Data.addVarInt(stataVarName);
			SFIToolkit.displayln("Added new Integer variable '" + stataVarName + "' to dataset.");
			break;
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.BIT:
			Data.addVarByte(stataVarName);
			SFIToolkit.displayln("Added new Byte variable '" + stataVarName + "' to dataset.");
			break;
		case java.sql.Types.DECIMAL:
		case java.sql.Types.NUMERIC:
		case java.sql.Types.DOUBLE:
			Data.addVarDouble(stataVarName);
			SFIToolkit.displayln("Added new Double variable '" + stataVarName + "' to dataset.");
			break;
		case java.sql.Types.FLOAT:
		case java.sql.Types.REAL:
			Data.addVarFloat(stataVarName);
			SFIToolkit.displayln("Added new Float variable '" + stataVarName + "' to dataset.");
			break;
		case java.sql.Types.LONGVARCHAR:
		case java.sql.Types.LONGNVARCHAR:
		case java.sql.Types.CLOB:
		case java.sql.Types.NCLOB:
			Data.addVarStrL(stataVarName);
			SFIToolkit.displayln("Added new StrL variable '" + stataVarName + " to dataset.");
			break;
		case java.sql.Types.CHAR:
		case java.sql.Types.NCHAR:
		case java.sql.Types.NVARCHAR:
		case java.sql.Types.VARCHAR:
			Data.addVarStr(stataVarName, res.getMetaData().getColumnDisplaySize(i));
			SFIToolkit.displayln("Added new Str variable '" + stataVarName + "' of legth " + res.getMetaData().getColumnDisplaySize(i) + " to dataset.");
			break;
		case java.sql.Types.DATE:
			Data.addVarStr(stataVarName, 10);
			SFIToolkit.displayln("Added new Str variable '" + stataVarName + "' of legth 10 to dataset.");
			break;
		case java.sql.Types.TIME:
			Data.addVarStr(stataVarName, 15);
			SFIToolkit.displayln("Added new Str variable '" + stataVarName + "' of legth 15 to dataset.");
			break;
		case java.sql.Types.TIME_WITH_TIMEZONE:
			Data.addVarStr(stataVarName, 20);
			SFIToolkit.displayln("Added new Str variable '" + stataVarName + "' of legth 20 to dataset.");
			break;
		case java.sql.Types.TIMESTAMP:
			Data.addVarStr(stataVarName, 26);
			SFIToolkit.displayln("Added new Str variable '" + stataVarName + "' of legth 26 to dataset.");
			break;
		case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
			Data.addVarStr(stataVarName, 31);
			SFIToolkit.displayln("Added new Str variable '" + stataVarName + "' of legth 33 to dataset.");
			break;
		default:
			SFIToolkit.errorln("Unsupported result column type for column: " + stataVarName);
			SFIToolkit.errorln("Only numeric, string and date types are supported by Stata.");
			throw new Exception("Unsupported result column type for column: " + stataVarName);
		}
	}
}
