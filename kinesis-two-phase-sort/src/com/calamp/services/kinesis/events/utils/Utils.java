package com.calamp.services.kinesis.events.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.calamp.services.kinesis.events.data.CalAmpEvent;

public class Utils {

	public static AmazonKinesis kinesisClient;
	static final String pathToLastSeq = "lastSeq.bak";
	
	public static String getLastSeqNum( ){
		File f = new File(pathToLastSeq);
		if( f.exists() && !f.isDirectory() ) { 
			try {
				FileReader fr = new FileReader( f.getAbsoluteFile() );
				BufferedReader br = new BufferedReader(fr);
				String key = br.readLine();
				br.close();
				return key;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	public static void writeLastSeqNum(String last){
		File f = new File(pathToLastSeq);
		try {
			FileWriter fw = new FileWriter( f.getAbsoluteFile(), false );
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write( String.format("%s%n", last) );
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName Name of stream
     */
    public static void validateStream(AmazonKinesis kinesisClient, String streamName) {
        try {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            if(!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        } catch (ResourceNotFoundException e) {
            System.err.println("Stream " + streamName + " does not exist. Please create it in the console.");
            System.err.println(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }
    
    public static void lazyLog(PutRecordRequest putRecord, String logPath) {
    	String myStr = "PUT TO [" + putRecord.getStreamName() + "] ";
    	myStr += " Seq-ID: " + putRecord.getSequenceNumberForOrdering();
    	myStr += " Part-K: " + putRecord.getPartitionKey();
    	myStr += " Data: " + CalAmpEvent.fromJsonAsBytes( putRecord.getData().array() );
    	LazyLogger.log(logPath, true, myStr);
    }
    public static void lazyLog(Record record, String stream, String logPath) {
    	String myStr = "GET AT [" + stream + "] ";
    	myStr += " Seq-ID: " + record.getSequenceNumber();
    	myStr += " Part-K: " + record.getPartitionKey();
    	myStr += " Data: " + CalAmpEvent.fromJsonAsBytes( record.getData().array() );
    	LazyLogger.log(logPath, true, myStr);
    }
    public static void initLazyLog(String logPath, String initMessage) {
    	LazyLogger.log(logPath, false, initMessage);
    }
	public static void lazyLog(PutRecordsRequestEntry prre, String streamName, String logPath) {
    	String myStr = "PUT TO [" + streamName + "] ";
    	myStr += " Part-K: " + prre.getPartitionKey();
    	myStr += " Data: " + CalAmpEvent.fromJsonAsBytes( prre.getData().array() );
    	LazyLogger.log(logPath, true, myStr);
	}
}