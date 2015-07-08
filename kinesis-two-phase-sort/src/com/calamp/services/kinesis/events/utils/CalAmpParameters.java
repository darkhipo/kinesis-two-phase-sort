package com.calamp.services.kinesis.events.utils;

public final class CalAmpParameters {

	public final static Boolean alwaysPoll = false;
	public final static Integer pollDelayMillis = 1;//5000;
	public final static Integer maxRecordsPerPut = 500; //Kinesis variable, must be 500.
	public final static Integer minimumAgeMillis = 2000;
	public final static Integer writerSleepMillis = 1000;
	public final static Integer maxRecPerPoll = 10000; //Amazon says 10000 is max.
	public final static Integer randomMillisWindow = 3000;
	public final static String unorderdStreamName = "unordered-message-stream"; 
	public final static String orderedStreamName = "ordered-message-stream"; 
	public final static String regionName = "us-west-2";
	public final static String sortAppName = "sorting-buffer-app";
	public final static String consumeAppName = "consume-ordered-events-app";
	public final static String writeLogName = "kinesis-write.log";
	public final static String bufferLogName = "kinesis-buffer.log";
	public final static String readLogName = "kinesis-read.log";
}
