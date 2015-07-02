package darkhipo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

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
    
    /**
     * Uses the Kinesis client to send the stock trade to the given stream.
     *
     * @param trade instance representing the stock trade
     * @param kinesisClient Amazon Kinesis client
     * @param streamName Name of stream
     */
    public static void sendEvent(Event trade, AmazonKinesis kinesisClient, String streamName, String prevSeqNum, Log LOG) {
        byte[] bytes = trade.toJsonAsBytes();
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for stock trade");
            return;
        }
        
        LOG.info("Putting trade: " + trade.toString());
        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        putRecord.setPartitionKey("OnePartition");//trade.getTickerSymbol()
        putRecord.setData(ByteBuffer.wrap(bytes));

        //This is needed to guaranteed FIFO ordering per partitionKey
        if (prevSeqNum != null){
        	 putRecord.setSequenceNumberForOrdering( prevSeqNum );
        }
        try {
        	String myStr = "";
        	myStr += " Seq-ID: " + putRecord.getSequenceNumberForOrdering();
        	myStr += " Part-K: " + putRecord.getPartitionKey();
        	myStr += " Data: " + trade;
        	PutRecordResult x = kinesisClient.putRecord(putRecord);
        	prevSeqNum = x.getSequenceNumber();
        	LazyLogger.log("kinesis-test-write.log", true, myStr);
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
}
