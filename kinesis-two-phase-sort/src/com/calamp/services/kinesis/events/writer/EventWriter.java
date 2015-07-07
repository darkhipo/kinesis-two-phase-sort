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

package com.calamp.services.kinesis.events.writer;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.calamp.services.kinesis.events.utils.ConfigurationUtils;
import com.calamp.services.kinesis.events.utils.CredentialUtils;
import com.calamp.services.kinesis.events.utils.CalAmpEvent;
import com.calamp.services.kinesis.events.utils.CalAmpParameters;
import com.calamp.services.kinesis.events.utils.Utils;

/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class EventWriter {
	static String prevSeqNum = null;
    private static final Log LOG = LogFactory.getLog(EventWriter.class);

    private static void checkUsage(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: " + EventWriter.class.getSimpleName()
                    + " <stream name> <region>");
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
    public static void sendEvent(CalAmpEvent trade, AmazonKinesis kinesisClient, String streamName ) {
        byte[] bytes = trade.toJsonAsBytes();
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for stock trade");
            return;
        }
        
        LOG.info("Putting trade: " + trade.toString());
        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(CalAmpParameters.unorderdStreamName);
        putRecord.setPartitionKey(trade.getTickerSymbol());
        putRecord.setData(ByteBuffer.wrap(bytes));

        //This is needed to guaranteed FIFO ordering per partitionKey
        if (prevSeqNum != null){
        	 putRecord.setSequenceNumberForOrdering( prevSeqNum );
        }
        try {
        	PutRecordResult res = kinesisClient.putRecord(putRecord);
        	prevSeqNum = res.getSequenceNumber();
        	Utils.lazyLog(putRecord, CalAmpParameters.writeLogName);
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
    
    public static void main(String[] args) throws Exception {
        checkUsage(args);
        String streamName = CalAmpParameters.unorderdStreamName; //args[0];
        String regionName = CalAmpParameters.regionName;
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }
        AWSCredentials credentials = CredentialUtils.getCredentialsProvider().getCredentials();

        ClientConfiguration ccuo = ConfigurationUtils.getClientConfigWithUserAgent(true);
        AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials, ccuo);
        kinesisClient.setRegion(region);

        // Validate that the stream exists and is active
        Utils.validateStream(kinesisClient, streamName);

        Utils.initLazyLog(CalAmpParameters.writeLogName, "Producer Start");
        
        // Repeatedly send stock trades with a some milliseconds wait in between
        EventGenerator stockTradeGenerator = new EventGenerator();
        while( true ) {
            CalAmpEvent trade = stockTradeGenerator.getRandomTrade();
            sendEvent(trade, kinesisClient, streamName);
            Thread.sleep(com.calamp.services.kinesis.events.utils.CalAmpParameters.writerSleepMillis);
        }
    }
}
