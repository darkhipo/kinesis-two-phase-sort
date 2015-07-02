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

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.CredentialUtils;
import com.amazonaws.services.kinesis.samples.stocktrades.writer.EventGenerator;

import darkhipo.Event;
import darkhipo.LazyLogger;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class RecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(RecordProcessor.class);
    private String kinesisShardId;
    private Boolean isUnordered;

    public RecordProcessor(Boolean isUnordered) {
    	this.isUnordered = isUnordered;
    	Region region = RegionUtils.getRegion("us-west-2");
    	
    	if (this.isUnordered){
    		AWSCredentials credentials;
			try {
				credentials = CredentialUtils.getCredentialsProvider().getCredentials();
		        AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials, ConfigurationUtils.getClientConfigWithUserAgent());
		        kinesisClient.setRegion(region);
		        darkhipo.Utils.validateStream(kinesisClient, darkhipo.Parameters.outStreamName);
			} catch (Exception e) {
				e.printStackTrace();
			}

            //com.amazonaws.services.kinesis.samples.stocktrades.writer.EventWriter.sendStockTrade(trade, kinesisClient, outStreamName);
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
    	System.out.println("Process Records #" + records.size());    	
    	List<darkhipo.Event> eList = Collections.synchronizedList(new ArrayList<darkhipo.Event>());
    	Collections.sort(eList, new darkhipo.EventComparator());
    	checkpoint(checkpointer);
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