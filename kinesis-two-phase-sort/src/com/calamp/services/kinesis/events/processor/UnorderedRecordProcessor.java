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
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.Record;
import com.calamp.services.kinesis.events.utils.CalAmpEventPriorityComparator;
import com.calamp.services.kinesis.events.utils.ConfigurationUtils;
import com.calamp.services.kinesis.events.utils.CredentialUtils;
import com.calamp.services.kinesis.events.utils.CalAmpEvent;
import com.calamp.services.kinesis.events.utils.CalAmpEventFilter;
import com.calamp.services.kinesis.events.utils.CalAmpParameters;
import com.calamp.services.kinesis.events.utils.Utils;

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
			Region region = RegionUtils.getRegion(CalAmpParameters.regionName);
			AWSCredentials credentials = CredentialUtils.getCredentialsProvider().getCredentials();
			ClientConfiguration ccuord = ConfigurationUtils.getClientConfigWithUserAgent(true);
			ClientConfiguration ccord = ConfigurationUtils.getClientConfigWithUserAgent(false);
			kinesisClientToUnordered = new AmazonKinesisClient(credentials, ccuord);
			kinesisClientToOrdered = new AmazonKinesisClient(credentials, ccord);
			kinesisClientToUnordered.setRegion(region);
			kinesisClientToOrdered.setRegion(region);
			Utils.validateStream(kinesisClientToUnordered, CalAmpParameters.unorderdStreamName);
			Utils.validateStream(kinesisClientToOrdered, CalAmpParameters.orderedStreamName);
	        Utils.initLazyLog(CalAmpParameters.bufferLogName, "Sort Buffer Start");
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
		List<CalAmpEvent> eventsOldEnough = Collections.synchronizedList( new ArrayList<CalAmpEvent>() );
		List<CalAmpEvent> eventsTooYoung = Collections.synchronizedList( new ArrayList<CalAmpEvent>() );
		for (Record r : records){    
			Utils.lazyLog(r, CalAmpParameters.unorderdStreamName, CalAmpParameters.bufferLogName);
	        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
	        byte[] bytes = r.getData().array();
			if (bytes != null ) {
		        CalAmpEvent e = CalAmpEvent.fromJsonAsBytes( r.getData().array() );
				if (e != null ) {
			        if ( !(eventsOldEnough.contains(e)) ){
			        	if ( CalAmpEventFilter.oldEnough(e) ){
			        		eventsOldEnough.add(e);
			        	}
			        	else if ( !(eventsTooYoung.contains(e)) ){
			        		eventsTooYoung.add(e);
			        	}
			        }
				}
		        else{
		        	LOG.warn("Skipping record. Unable to parse record into CalAmpEvent 2. Partition Key: " + r.getPartitionKey() + " Event: " + e);
		        }
			}
			else{
				LOG.warn("Skipping record. Unable to parse record into CalAmpEvent 1. Partition Key: " + r.getPartitionKey());
			}
		}
		
		Collections.sort( eventsOldEnough, new CalAmpEventPriorityComparator() );
		Collections.sort( eventsTooYoung, new CalAmpEventPriorityComparator() );
		
		for ( CalAmpEvent cae : eventsTooYoung ){
			LOG.info("Event too young : " + cae);
			
		}
		for ( CalAmpEvent cae : eventsOldEnough ){
			LOG.info("Event old enough: " + cae);
		}
		
		putByParts( eventsTooYoung, CalAmpParameters.unorderdStreamName, kinesisClientToUnordered);
		putByParts( eventsOldEnough, CalAmpParameters.orderedStreamName, kinesisClientToOrdered);
		checkpoint(checkpointer);  
    }

	private void putByParts(List<CalAmpEvent> events, String streamName, AmazonKinesis kc) {
		List<PutRecordsRequestEntry> prres = Collections.synchronizedList( new ArrayList<PutRecordsRequestEntry>() );
		for (CalAmpEvent e : events){
			PutRecordsRequestEntry prre = new PutRecordsRequestEntry().withData(ByteBuffer.wrap(e.toJsonAsBytes()));
			prre.setPartitionKey( e.getTickerSymbol() );
			prres.add(prre);
			Utils.lazyLog(prre, streamName, CalAmpParameters.bufferLogName);
		}
		if (prres.size() > 0){
			int requestNumber = ( events.size() / CalAmpParameters.maxRecordsPerPut );
			requestNumber += (events.size() % CalAmpParameters.maxRecordsPerPut) == 0 ? 0 : 1;
			for (int j=0; j<requestNumber; j++){
				PutRecordsRequest putRecords = new PutRecordsRequest( ).withRecords(prres);
				putRecords.setStreamName(streamName);
				PutRecordsResult prr = kc.putRecords(putRecords);
				
				/* Retry failed "record puts" until success.
				 */
				while (prr.getFailedRecordCount() > 0) {
				    final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
				    final List<PutRecordsResultEntry> putRecordsResultEntryList = prr.getRecords();
				    for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
				        final PutRecordsRequestEntry putRecordRequestEntry = prres.get(i);
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