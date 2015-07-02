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

package com.amazonaws.services.kinesis.samples.stocktrades.writer;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.CredentialUtils;

import darkhipo.Event;
import darkhipo.LazyLogger;

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

    public static void main(String[] args) throws Exception {
        checkUsage(args);
        String streamName = darkhipo.Parameters.inStreamName; //args[0];
        String regionName = darkhipo.Parameters.regionName;
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }

        AWSCredentials credentials = CredentialUtils.getCredentialsProvider().getCredentials();

        AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials, ConfigurationUtils.getClientConfigWithUserAgent());
        kinesisClient.setRegion(region);

        // Validate that the stream exists and is active
        darkhipo.Utils.validateStream(kinesisClient, streamName);

        LazyLogger.log("kinesis-test-write.log", false, "Producer Start");
        // Repeatedly send stock trades with a some milliseconds wait in between
        EventGenerator stockTradeGenerator = new EventGenerator();
        while(true) {
            Event trade = stockTradeGenerator.getRandomTrade();
            darkhipo.Utils.sendEvent(trade, kinesisClient, streamName, prevSeqNum, LOG);
            Thread.sleep(1000);
        }
    }

}
