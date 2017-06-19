package DB.BL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.swing.JLabel;
import javax.swing.JOptionPane;

import DB.BL.ConstantsClassDB;
import DB.BL.ThreadDBConnector;

public class DBManager {
	private Connection conToDB = null;
	private Statement stmtSelect = null, stmtUpdate = null;
	private ResultSet resultSet = null;
	private String strDBUrl = "", strDBDriver = "", strDBUserName = "", strDBPassword = "", strQueryResult = "";
	private PreparedStatement pStmtSelect = null;

	private JLabel lblDBConnectionStatus;

	private ThreadDBConnector threadDBConnector = null;

	public DBManager(String DBServerIP, String DBPortNo, String DBUserName, String DBPassword, String DBName) {
		System.out.println("I m in.");
		// MS SQL
		// strDBUrl = "jdbc:jtds:sqlserver://" + DBServerIP + ":" + DBPortNo +
		// "/" + DBName + ";instance=SQLEXPRESS";
		// strDBUrl = "jdbc:sqlserver://" + DBServerIP + ":" + DBPortNo +
		// ";databaseName=" + DBName + ";user=" + DBUserName + ";password=" +
		// DBPassword;
		// System.out.println(strDBUrl);

		// MySQL
		strDBUrl = "jdbc:mysql://" + DBServerIP + ":" + DBPortNo + "/" + DBName;

		// MS SQL
		// strDBDriver = "net.sourceforge.jtds.jdbc.Driver";
		// strDBDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

		// MySQL
		strDBDriver = "com.mysql.jdbc.Driver";

		strDBUserName = DBUserName;
		strDBPassword = DBPassword;

		// ConstantsClassDB.dbManager = this;
	}

	public boolean connectToDB(JLabel lblStatus) {

		lblDBConnectionStatus = lblStatus;

		// Don't use ProgressThread here! It'll shoot up the processor with
		// excess load of recursive threading.
		if (lblDBConnectionStatus != null)
			lblDBConnectionStatus.setText("    Connecting to database server...");

		try {
			Class.forName(strDBDriver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}

		try {
			conToDB = DriverManager.getConnection(strDBUrl, strDBUserName, strDBPassword);
		} catch (SQLException e) {
			// Reconnecting...
			e.printStackTrace();
			return false;
		}

		if (lblDBConnectionStatus != null)
			lblDBConnectionStatus.setText("    Connection established!");

		ConstantsClassDB.boolIsDBManuallyConnected = true;
		ConstantsClassDB.dbManager = this;

		System.out.println("Connected!");

		return true;
	}

	public String getStringFromDB(String Query) {

		resultSet = null;
		strQueryResult = "";
		try {
			stmtSelect = conToDB.createStatement();
			resultSet = stmtSelect.executeQuery(Query);
			resultSet.next();

			if (resultSet.getRow() != 0)
				strQueryResult = resultSet.getString(1);

			resultSet.close();
			stmtSelect.close();
		} catch (SQLException e) {
			autoReconnectToBDserver(e, false);

			return e.getMessage();
		}

		return strQueryResult;
	}

	public ResultSet getDataFromDB(String Query) {

		resultSet = null;
		try {
			pStmtSelect = conToDB.prepareStatement(Query);
			resultSet = pStmtSelect.executeQuery();
		} catch (SQLException e) {

			autoReconnectToBDserver(e, true);

			return null;
		}

		return resultSet;
	}

	public String getStringFromDBSecured(String Query, ArrayList<String> QueryParameters) { // To
																							// prevent
																							// SQL
																							// injection
																							// attacks
																							// -
																							// Use
																							// especially
																							// on
																							// Login
																							// form

		resultSet = null;
		strQueryResult = "";
		try {
			pStmtSelect = conToDB.prepareStatement(Query);

			for (int i = 0; i < QueryParameters.size(); i++) {
				pStmtSelect.setString((i + 1), QueryParameters.get(i));
			}

			resultSet = pStmtSelect.executeQuery();
			resultSet.next();

			if (resultSet.getRow() != 0)
				strQueryResult = resultSet.getString(1);

			resultSet.close();
			pStmtSelect.close();
		} catch (SQLException e) {
			autoReconnectToBDserver(e, false);

			return e.getMessage();
		}

		return strQueryResult;

	}

	public String executeQuery(String Query) {
		try {
			stmtUpdate = conToDB.createStatement();

			stmtUpdate.executeUpdate(Query);

			stmtUpdate.close();
		} catch (SQLException e) {
			autoReconnectToBDserver(e, false);

			return e.getMessage();
		}

		return null;
	}

	private void autoReconnectToBDserver(SQLException e, boolean ShowErrMsg) {
		if (e.getSQLState().equals("HY010") || e.getSQLState().startsWith("08")) {
			if (!ConstantsClassDB.boolIsThreadRunning) {
				ConstantsClassDB.boolIsThreadRunning = true;
				threadDBConnector = new ThreadDBConnector(this, lblDBConnectionStatus);
				threadDBConnector.start();
			}
		} else {
			if (ShowErrMsg)
				JOptionPane.showMessageDialog(null, e.getMessage(), "Error Message", JOptionPane.ERROR_MESSAGE);
		}
	}

	public void closeConnection() {
		if (conToDB != null) {
			try {
				conToDB.close();
			} catch (SQLException e) {
				e.printStackTrace();
				JOptionPane.showMessageDialog(null, e.getMessage(), "Error Message", JOptionPane.ERROR_MESSAGE);
			}
		}
	}

}
