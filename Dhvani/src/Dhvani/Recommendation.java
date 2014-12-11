package Dhvani;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Recommendation 
{
	 private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	 
	  /**
	   * @param args
	   * @throws SQLException
	   */
	  
	 public static void findRecommendation(String codeStr) throws SQLException
	 {
		 
		 //Storing the code string passed as parameter
		 
		 String codeString = codeStr;
		 
	      try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	      System.exit(1);
	    }
	      
	      // setting the connection to hive2 server
	      Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "DhvaniUser1", "");
	      Statement stmt = connection.createStatement();
	      String dbTableName = "UserTable";
	      
	      //drop reference table if already exists
	      stmt.execute("drop table if exists " + dbTableName);
	      
	      //create external reference Hive table to refer data from DynamoDB
	      stmt.execute("create external table " + dbTableName + " (HiveUserId int, HivePlaylistArray ArrayList<String>, "+
	       "HiveTimeArray ArrayList<int>)" ); //+
	      " Stored by 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'" +
	      "TBLPROPERTIES (" + '"' + "dynamodb.table.name" + '" =' + ""DhvaniUserTable", "dynamodb.column.mapping" " " +
	      		+" = "HiveUserId : userId, HivePlaylistArray: PlaylistArray, HiveTimeArray:timeArray" )";
	      
	      String sql = "show tables '" + dbTableName + "'";
	      System.out.println("Running: " + sql);
	      ResultSet rset = stmt.executeQuery(sql);
	      if (rset.next()) {
	        System.out.println(rset.getString(1));
	      }
	      
	 }
}
