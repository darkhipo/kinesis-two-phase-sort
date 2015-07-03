package com.calamp.services.kinesis.events.utils;

public final class Parameters {

	public final static String unorderdStreamName = "unordered-message-stream"; 
	public final static String orderedStreamName = "ordered-message-stream"; 
	public final static String regionName = "us-west-2";
	public final static int pollDelayMillis = 5000;
	public final static int maxRecordsPerPut = 500;
	public final static int minimumAgeMillis = 2000;
	public final static int writerSleepMillis = 1000;
	public final static int maxRecPerPoll = 10000;
	public final static String sortAppName = "sorting-buffer-app";
	public final static String consumeAppName = "consume-ordered-events-app";
	public final static String writeLogName = "kinesis-test-write.log";
	public final static String readLogName = "kinesis-test-read.log";
}
