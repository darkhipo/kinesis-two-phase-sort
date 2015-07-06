/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.calamp.services.kinesis.events.processor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.calamp.services.kinesis.events.utils.ConfigurationUtils;
import com.calamp.services.kinesis.events.utils.CredentialUtils;
import com.calamp.services.kinesis.events.utils.Event;
import com.calamp.services.kinesis.events.utils.EventAgeTest;
import com.calamp.services.kinesis.events.utils.Parameters;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class UnorderedRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(UnorderedRecordProcessor.class);
    private String kinesisShardId;
    private AmazonKinesis kinesisClientToOrdered;
    private AmazonKinesis kinesisClientToUnordered;

    public UnorderedRecordProcessor() {
		try {
			Region region = RegionUtils.getRegion(com.calamp.services.kinesis.events.utils.Parameters.regionName);
			AWSCredentials credentials = CredentialUtils.getCredentialsProvider().getCredentials();
			ClientConfiguration ccuord = ConfigurationUtils.getClientConfigWithUserAgent(true);
			ClientConfiguration ccord = ConfigurationUtils.getClientConfigWithUserAgent(false);
			kinesisClientToUnordered = new AmazonKinesisClient(credentials, ccuord);
			kinesisClientToOrdered = new AmazonKinesisClient(credentials, ccord);
			kinesisClientToUnordered.setRegion(region);
			kinesisClientToOrdered.setRegion(region);
			com.calamp.services.kinesis.events.utils.Utils.validateStream(kinesisClientToUnordered, com.calamp.services.kinesis.events.utils.Parameters.unorderdStreamName);
			com.calamp.services.kinesis.events.utils.Utils.validateStream(kinesisClientToOrdered, com.calamp.services.kinesis.events.utils.Parameters.orderedStreamName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		System.out.println("Process Unordered Records #" + records.size());   
		List<Event> eventsOldEnough = Collections.synchronizedList( new ArrayList<Event>() );
		List<Event> eventsTooYoung = Collections.synchronizedList( new ArrayList<Event>() );
		for (Record r : records){    		
	        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
	        byte[] bytes = r.getData().array();
			if (bytes == null ) {
	    	    LOG.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: " + r.getPartitionKey());
	        }
	        Event e = Event.fromJsonAsBytes( r.getData().array() );
	        if( EventAgeTest.oldEnough(e) ){
	        	eventsOldEnough.add(e);
	        }
	        else{
	        	eventsTooYoung.add(e);
	        }
		}
		Collections.sort( eventsOldEnough, new com.calamp.services.kinesis.events.utils.EventComparator() );
		Collections.sort( eventsTooYoung, new com.calamp.services.kinesis.events.utils.EventComparator() );
		putByParts( eventsOldEnough, Parameters.orderedStreamName, kinesisClientToOrdered);
		putByParts( eventsTooYoung, Parameters.unorderdStreamName, kinesisClientToUnordered);
		checkpoint(checkpointer);  
    }

	private void putByParts(List<Event> events, String streamName, AmazonKinesis kc) {
		List<PutRecordsRequestEntry> prres = Collections.synchronizedList( new ArrayList<PutRecordsRequestEntry>() );
		for (Event e : events){
			PutRecordsRequestEntry prre = new PutRecordsRequestEntry().withData(ByteBuffer.wrap(e.toJsonAsBytes()));
			prre.setPartitionKey("OnePartition");//trade.getTickerSymbol()
			prres.add(prre);
		}
		if (prres.size() > 0){
			int requestNumber = ( events.size() / Parameters.maxRecordsPerPut );
			requestNumber += (events.size() % Parameters.maxRecordsPerPut) == 0 ? 0 : 1;
			for (int i=0; i<requestNumber; i++){
				PutRecordsRequest putRecords = new PutRecordsRequest( ).withRecords(prres);
				putRecords.setStreamName(streamName);
				PutRecordsResult prr = kc.putRecords(putRecords);
			}
		}
	}
	
    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }
}