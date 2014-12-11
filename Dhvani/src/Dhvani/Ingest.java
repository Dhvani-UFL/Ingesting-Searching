/** 
 * 
 * 
 *
 *  
 *
 * 
 */

package Dhvani;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;

// Import JSON related libraries

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Ingest {

	/*
     * Important: Be sure to fill in your AWS access credentials in the
     *            AwsCredentials.
     * http://aws.amazon.com/security-credentials
     */

    static AmazonDynamoDBClient dynamoDB;

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
                AmazonDynamoDBSample.class.getResourceAsStream("AwsCredentials.properties"));

        dynamoDB = new AmazonDynamoDBClient(credentials);
    }


    public static void main(String[] args) throws Exception 
    {
        init();
        
        // This method takes the file name as parameter. The file is in JSON format
        
        //decalring the variables for storing the fields extracted from the JSON formatted files.
        
        String code;
        String track_id;
        int length;
        String version;
        String artist;
        String title;
        String release;
        
        JSONArray songList = (JSONArray) parser.parse(new FileReader(args[0]));
        
        for (Object songObject : songList)
                


        try {
            String tableName = "DhvaniSongsTable";

                        // Describe our new table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: " + tableDescription);

          

            // Scan database for presence of these fingerprints
            HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue().withN("1985"));
            scanFilter.put("year", condition);
            ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
            ScanResult scanResult = dynamoDB.scan(scanRequest);
            System.out.println("Result: " + scanResult);

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private static Map<String, AttributeValue> newItem(String track_id,int length,String version,String artist,String title,String release) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("track_id", new AttributeValue(track_id));
        item.put("length", new AttributeValue().withN(Integer.toString(length)));
        item.put("version", new AttributeValue(version));
        item.put("artist", new AttributeValue(artist));
        item.put("title", new AttributeValue(title));
        item.put("release", new AttributeValue(release));

        return item;
    }
    
    private static Map<String, AttributeValue> newCode(String track_id,int hashKeyCode,int timeStamp)
    {
        Map<String, AttributeValue> hashCode = new HashMap<String, AttributeValue>();
                		
        		//insert the track ID
        		hashCode.put("track_id", new AttributeValue(track_id));
        		
        		//insert the hash key code
        		hashCode.put("hashKeyCode", new AttributeValue().withN(Integer.toString(hashKeyCode));
        		
        		//insert the timestamp of that hash key code
        		hashCode.put("timeStamp", new AttributeValue().withN(Integer.toString(timeStamp)));
       
        }
        
       // return the item with Track ID and Hash Key inserted
        return hashCode;
    }

    private static void putItemIntoTable(String code, String track_id,int length,String version,String artist,String title,String release)
    {
    	  // Add an item
        Map<String, AttributeValue> songRecord = newItem(track_id, length, version, artist, title, release);
        
        //Set table name to the Songs table
        
        tableName = "DhvaniSongsTable";
        PutItemRequest putItemsRequest = new PutItemRequest(tableName, songRecord);
        PutItemResult putItemsResult = dynamoDB.putItem(putItemsRequest);
        System.out.println("Result: " + putItemsResult);
        
        //Set table name to the hash code table
        tableName = "DhvaniHashCodeTable";
        
		StringTokenizer hashKeyToken = new StringTokenizer(code,",");
		        
		        while(hashKeyToken.hasMoreTokens())
		        {
		        	String hashKey = hashKeyToken.nextToken();
		        	StringTokenizer timeToken = new StringTokenizer(hashKey,":");
		        	
		        	while(timeToken.hasMoreTokens())
		        	{
		        		//extract the hash Key from the string.
		        		
		        		int hashKeyCode = Integer.parseInt(timeToken.nextToken());
		        		
		        		//extract the time stamp from the string.
		        		int timeStamp = Integer.parseInt(timeToken.nextToken());
		        		
		        		//insert hashcode into DhvaniHashCode Table
		        		Map<String, AttributeValue> hashKeyRecord = newCode(track_id, hashKeyCode,timeStamp );
		        		
		        		PutItemRequest putHashCodeRequest = new PutItemRequest(tableName, hashKeyRecord);
		                PutItemResult putHashResult = dynamoDB.putItem(putHashCodeRequest);
		                System.out.println("Result: " + putHashResult);
		        	}

    }
    private static void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }

}
