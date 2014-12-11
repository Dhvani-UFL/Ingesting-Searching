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


public class SearchFP 
{
	 private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	 
	 String finalSongData;
	  /**
	   * @param args
	   * @throws SQLException
	   */
	 
	 public String searchFPrint(String codeStr) throws SQLException
	 {
		 
		 //Store the code string passed as parameter
		 String codeString = codeStr;
		 
	      try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	      System.exit(1);
	    }
	      
	      // setting the connection to hive2 server
	      
	      String server = "localhost";
	      String port = "10000";
	      
	      Connection connection = DriverManager.getConnection("jdbc:hive2://"+ server+":"+port +"/default", "DhvaniUser1", "");
	      Statement stmt = connection.createStatement();
	      String dbTableName = "HiveDhvaniHashCodeTable";
	      
	      //drop reference table if already exists
	      stmt.execute("drop table if exists " + dbTableName);
	      
	      //create external reference Hive table to refer data from DynamoDB
	      stmt.execute("create external table " + dbTableName + " (HiveHashKey int, HiveTracksArray ArrayList<String>, "+
	       "HiveTimeArray ArrayList<int>)" +
	      " Stored by 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'" +
	      "TBLPROPERTIES (" + '"' + "dynamodb.table.name" + '" =' + ""DhvaniHashCodeTable", "dynamodb.column.mapping" " " +
	      		+" = " HiveHashKey: HashKey, HiveTracksArray: tracksArray, HiveTimeArray:timeArray" )";
	      
	      String sql = "show tables '" + dbTableName + "'";
	      System.out.println("Running: " + sql);
	      ResultSet rset = stmt.executeQuery(sql);
	      if (rset.next()) {
	        System.out.println(rset.getString(1));
	      }
	      
	      
	      //create table for queried audio hash keys
	      
	      String QueryHashTable = "HiveQueryHashTable";
	      stmt.execute("drop table if exists " + QueryHashTable);
	      stmt.execute("create table " + QueryHashTable + " (qryHash int, qryTime int)");
	      
	      String qryHashStamp = null;
	      String qryHashkey = "(";
	      
	      ArrayList<Integer> qryTimeArray = new ArrayList<Integer>();
	      ArrayList<Integer> qryHashArray = new ArrayList<Integer>();
	      
	      StringTokenizer qryHashToken = new StringTokenizer(codeString,",");
	        
	        while(qryHashToken.hasMoreTokens())
	        {
	        	String qryHashKey = qryHashToken.nextToken();
	        	StringTokenizer qryTimeToken = new StringTokenizer(qryHashKey,":");
	        	
	        	while(qryTimeToken.hasMoreTokens())
	        	{
	        		//extract the hash Key from the string.
	        		
	        		int hashKeyCode = Integer.parseInt(qryTimeToken.nextToken());
	        		
	        		qryHashkey =qryHashkey +" ," + hashKeyCode ;
	        		
	        		qryHashArray.add(hashKeyCode);
	        		
	        		//extract the time stamp from the string.
	        		int timeStamp = Integer.parseInt(qryTimeToken.nextToken());
	        		qryTimeArray.add(timeStamp);
	        		
	        		qryHashStamp = qryHashStamp + ", (" + hashKeyCode + "," + timeStamp + ")" ;
	        	}
	        }
	        
	        qryHashkey =qryHashkey +" )";
	      
	        //stmt.execute("Insert into "+ QueryHashTable )
	        
	      // search for hash Keys in the database
	      
	      String SelectSQL = "Select HiveHashKey, HiveTimeArray, HiveTracksArray from  " + dbTableName + 
	    		  " Where HiveHashKey in " +  qryHashkey ;
	      
	      ResultSet qryResult1 = stmt.executeQuery(SelectSQL);
	      ResultSet qryResult2 = qryResult1;
	      
	      ArrayList<Integer> timeDiffHist = new ArrayList<Integer>();
	      ArrayList<Integer> timeDiffArr = new ArrayList<Integer>();
	      
	      int timeDiff = 0;
	      
	      while(qryResult1.next())
	      {
	    	  int resHashKey = qryResult1.getInt("HiveHashKey");
	    	  
	    	  int index = qryHashArray.indexOf(resHashKey);
	    	  
	    	  ArrayList<Integer> matTimeArray = (ArrayList<Integer>) qryResult1.getArray("HiveTimeArray");
	    	  
	    	  Iterator timeIterate = matTimeArray.iterator();
	    	  
	    	  
	    	  // build a histogram of time differences for matched hash keys
	    	  
	    	  while(timeIterate.hasNext())
	    	  {
	    		   timeDiff =(int) timeIterate.next() - qryTimeArray.get(index);
	    		
	    		  timeDiffArr.add(timeDiff);
	    		  
	    		  int indexTimeDiff = timeDiffArr.indexOf(timeDiff);
	    		  timeDiffHist.set(indexTimeDiff, timeDiffHist.get(indexTimeDiff)+1);
	    		  
	    	  }
	    	  
	      }
	      
	      //Collections.sort(timeDiffHist,Collections.reverseOrder());
	     
	      //identify the top 2 histogram time offset values
	      
	      int highest = Integer.MIN_VALUE+1; 
	      int sec_highest = Integer.MIN_VALUE;
	      
	    //timeDiffHist is array of integers
	      
	      for(int i : timeDiffHist) 
	      {
	          if(i>highest)
	          {
	             
	        	//make current highest to second highest
	        	  sec_highest = highest; 
	             
	        	//make current value to highest
	        	  highest = i; 
	          }
	          else if(i>sec_highest) 
	          {
	             sec_highest = i;
	          }
	      }
	     
	      
	      //check if there is a significant difference between the top 2 histogram values.
	      
	      if((highest - sec_highest) > (highest/2))
	      {
	    	//select the top time difference value
	    	  int highTimeDiff= timeDiffArr.get(timeDiffHist.indexOf(highest));
	      
	      
	      
	      
	      ArrayList<String> listTrackName = new ArrayList<String>(); 
	      ArrayList<Integer> listTrackCount= new ArrayList<Integer>();
	      String trackName = "";
	      
	      while(qryResult2.next())
	      {
	    	  int resHashKey = qryResult2.getInt("HiveHashKey");
	    	  
	    	  int index = qryHashArray.indexOf(resHashKey);
	    	  
	    	  ArrayList<Integer> matTimeArray2 = (ArrayList<Integer>) qryResult2.getArray("HiveTimeArray");
	    	  ArrayList<String> matTrackName = (ArrayList<String>) qryResult2.getArray("HiveTracksArray");
	    	  Iterator timeIterate = matTimeArray2.iterator();
	    	  
	    	  for (int j:matTimeArray2 )
	    	  {
	    		  if(highTimeDiff ==((int) timeIterate.next() - qryTimeArray.get(index)))
	    		  {
	    			  trackName=matTrackName.get(index);
	    			  listTrackName.add(trackName);
	    			  int trackIndex = listTrackName.indexOf(trackName);
	    			  
	    			  listTrackCount.set(trackIndex, listTrackCount.get(trackIndex)+1);
	    		  }
	    		  
	    	  }
	    	  
	    	  	    	  
	      }
	      
	      int highest2 = Integer.MIN_VALUE+1; 
	      int sec_highest2 = Integer.MIN_VALUE;
	      
	    //listTrackCount is array of count of song matches.
	      
	      for(int i : listTrackCount) 
	      {
	          if(i>highest2)
	          {
	             
	        	//make current highest to second highest
	        	  sec_highest2 = highest2; 
	        	  
	        	  //make current value to highest
	             highest2 = i;
	          }
	          else if(i>sec_highest2) 
	          {
	             sec_highest2 = i;
	          }
	      }
	    
	      //identify the top song with highest matches
	      
	      String finalMatchingTrack= listTrackName.get(listTrackCount.indexOf(highest2));
	      
	      // call the getMetadata method to retrieve the meta data
	      finalSongData = getMetadata (finalMatchingTrack);
	      }
	      
	      
	      // if no proper histogram difference, it means song not found
	      
	      else
	      {
	    	  finalSongData = " ";
	      }
	      // retrieve the metadata of the identified song
	      
	      
	      return finalSongData;

	 }
	 
	 public String getMetadata(String SongName)
	 {
		 //create reference table for metadata table
	      
	      String metaDataTable = "HiveDhvaniSongsTable";
	      stmt.execute("drop table if exists " + metaDataTable);
	      
	      stmt.execute("create external table " + metaDataTable + " (HiveTrackId String, HiveTrackLength int, "+
	   	       "HiveArtist String,HiveTitle String, HiveRelease String )" +
	   	      " Stored by 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'" +
	   	      "TBLPROPERTIES (" + '"' + "dynamodb.table.name" + '" =' + ""DhvaniSongsTable", "dynamodb.column.mapping" " " +
	   	      		+" = " HiveTrackId: TrackId, HiveTrackLength: TrackLength, HiveArtist:Artist, +
	   	      		HiveTitle: Title, HiveRelease:Release" )";
	   	      
	      // search for meta data in the database
	      
	      String SelectSQL = "Select HiveTrackId, HiveTrackLength, HiveArtist,HiveTitle, HiveRelease   from  " + metaDataTable + 
	    		  " Where HiveTitle =" +  SongName +" limit 1" ;
	      
	      ResultSet qryResult3 = stmt.executeQuery(SelectSQL);
	      
	      //extract the metadata from the query result
	      while(qryResult3.next())
	      {
	    	  String trackID = qryResult3.getString("HiveTrackId");
	    	  int TrackLength = qryResult3.getInt("HiveTrackLength");
	    	  String Artist = qryResult3.getString("HiveArtist");
	    	  String Title = qryResult3.getString("HiveTitle");
	    	  String Release = qryResult3.getString("HiveRelease");
	    	  	    	  
	      }
	      
	      //concatenate the metadata into a predefined format
	      String metaData = Title + ":" + Artist+ ":"+ trackID +":" + TrackLength +":" + Release ;
	      return metaData;
	 }
}
