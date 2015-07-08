package com.calamp.services.kinesis.events.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
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
    
    public static void putByParts(List<CalAmpEvent> events, String streamName, AmazonKinesis kc, String logPath) {
		List<PutRecordsRequestEntry> prres = Collections.synchronizedList( new ArrayList<PutRecordsRequestEntry>() );
		for (CalAmpEvent e : events){
			PutRecordsRequestEntry prre = new PutRecordsRequestEntry().withData(ByteBuffer.wrap(e.toJsonAsBytes()));
			prre.setPartitionKey( String.valueOf( e.getMachineId() ) );
			prres.add(prre);
			Utils.lazyLog(prre, streamName, logPath);
		}
		
		if (prres.size() > 0){
			int requestNumber = ( events.size() / CalAmpParameters.maxRecordsPerPut );
			requestNumber += (events.size() % CalAmpParameters.maxRecordsPerPut) == 0 ? 0 : 1;
			Iterator<PutRecordsRequestEntry> it = prres.iterator();
			for (int j=0; j<requestNumber; j++){
				List<PutRecordsRequestEntry> payLoad = new ArrayList<PutRecordsRequestEntry>();
				while( it.hasNext() && payLoad.size() < CalAmpParameters.maxRecordsPerPut ){
					payLoad.add(it.next());
				}
				PutRecordsRequest putRecords = new PutRecordsRequest( ).withRecords(payLoad);
				putRecords.setStreamName(streamName);
				PutRecordsResult prr = kc.putRecords(putRecords);
				
				/** 
				 * Retry failed "record puts" until success.
				 */
				while (prr.getFailedRecordCount() > 0) {
				    final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
				    final List<PutRecordsResultEntry> putRecordsResultEntryList = prr.getRecords();
				    for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
				        final PutRecordsRequestEntry putRecordRequestEntry = payLoad.get(i);
				        final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
				        if (putRecordsResultEntry.getErrorCode() != null) {
				            failedRecordsList.add(putRecordRequestEntry);
				        }
				    }
				    prres = failedRecordsList;
				    putRecords.setRecords(prres);
				    prr = kc.putRecords(putRecords);
				}
			}
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