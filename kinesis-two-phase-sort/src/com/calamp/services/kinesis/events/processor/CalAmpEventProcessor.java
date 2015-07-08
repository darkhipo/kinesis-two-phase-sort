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

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.calamp.services.kinesis.events.utils.ConfigurationUtils;
import com.calamp.services.kinesis.events.utils.CredentialUtils;
import com.calamp.services.kinesis.events.utils.CalAmpParameters;

/**
 * Uses the Kinesis Client Library (KCL) to continuously consume and process stock trade
 * records from the stock trades stream. KCL monitors the number of shards and creates
 * record processor instances to read and process records from each shard. KCL also
 * load balances shards across all the instances of this processor.
 *
 */
public class CalAmpEventProcessor {

    private static final Log LOG = LogFactory.getLog(CalAmpEventProcessor.class);

    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER =
            Logger.getLogger("com.amazonaws.services.kinesis.samples.stocktrades.processor");
    
    private static void checkUsage(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: " + CalAmpEventProcessor.class.getSimpleName()
                    + " <application name> <stream name> <region> <isUnordered>");
            System.exit(1);
        }
    }

    /**
     * Sets the global log level to WARNING and the log level for this package to INFO,
     * so that we only see INFO messages for this processor. This is just for the purpose
     * of this tutorial, and should not be considered as best practice.
     *
     */
    private static void setLogLevels() {
        ROOT_LOGGER.setLevel(Level.INFO);
        PROCESSOR_LOGGER.setLevel(Level.INFO);
    }

    public static void main(String[] args) throws Exception {
        checkUsage(args);
        //String applicationName = args[0];
        //String streamName = args[1];
        //Region region = RegionUtils.getRegion(args[2]);
     	boolean isUnordered = Boolean.valueOf( args[3] );
        String applicationName = isUnordered ? CalAmpParameters.sortAppName : CalAmpParameters.consumeAppName;
       	String streamName = isUnordered ? CalAmpParameters.unorderdStreamName : CalAmpParameters.orderedStreamName;
       	Region region = RegionUtils.getRegion( CalAmpParameters.regionName );

        if (region == null) {
            System.err.println(args[2] + " is not a valid AWS region.");
            System.exit(1);
        }

        setLogLevels();
        AWSCredentialsProvider credentialsProvider = CredentialUtils.getCredentialsProvider();
        ClientConfiguration cc = ConfigurationUtils.getClientConfigWithUserAgent(true);
        AmazonKinesis kinesisClient = new AmazonKinesisClient(credentialsProvider, cc);
        kinesisClient.setRegion(region);
        
        com.calamp.services.kinesis.events.utils.Utils.kinesisClient = kinesisClient;
        
        String workerId = String.valueOf(UUID.randomUUID());
        KinesisClientLibConfiguration kclConfig =
             new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
            .withRegionName(region.getName())
            .withCommonClientConfig(cc)
            .withMaxRecords(com.calamp.services.kinesis.events.utils.CalAmpParameters.maxRecPerPoll)
            .withIdleTimeBetweenReadsInMillis(com.calamp.services.kinesis.events.utils.CalAmpParameters.pollDelayMillis)
            .withCallProcessRecordsEvenForEmptyRecordList(CalAmpParameters.alwaysPoll)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON); 

        IRecordProcessorFactory processorFactory = new RecordProcessorFactory( isUnordered );

        // Create the KCL worker with the stock trade record processor factory
        Worker worker = new Worker(processorFactory, kclConfig);

        int exitCode = 0;
        try {
        	worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
    }
}
