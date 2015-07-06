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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.calamp.services.kinesis.events.utils.CalAmpEvent;
import com.calamp.services.kinesis.events.utils.CalAmpParameters;
import com.calamp.services.kinesis.events.utils.Utils;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class OrderedRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(OrderedRecordProcessor.class);
    private String kinesisShardId;

    public OrderedRecordProcessor(){
    	Utils.initLazyLog(CalAmpParameters.readLogName, "Window Sorted Stream Start");
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
		System.out.println("Process Ordered Records #" + records.size());   
		for (Record record : records) {
	        // Final process record
			CalAmpEvent e = processRecord( record );
	    	LOG.info("ORDERED: " + e);
	    }
    	checkpoint(checkpointer);
    }

    private CalAmpEvent processRecord(Record record) {
    	CalAmpEvent e = CalAmpEvent.fromJsonAsBytes(record.getData().array());
    	if (e == null) {
    	    LOG.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: " + record.getPartitionKey());
    	    return null;
    	}
    	Utils.lazyLog(record, CalAmpParameters.orderedStreamName, CalAmpParameters.readLogName);
    	return e;
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