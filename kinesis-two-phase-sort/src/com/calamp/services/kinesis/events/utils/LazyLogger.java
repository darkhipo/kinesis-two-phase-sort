package com.calamp.services.kinesis.events.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class LazyLogger {

	public static void log(String fullPath, Boolean doAppend, String message) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(fullPath, doAppend));
			bw.write( ts() + "# " + message );
			bw.newLine();
			bw.flush();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally { 
			if (bw != null)
				try {
					bw.close();
				} catch (IOException ioe2) {
					ioe2.printStackTrace();
				}
		} 
	}
	
	public static String ts(){
		//Date currentTime = new Date();
		//SimpleDateFormat sdf = new SimpleDateFormat("SSS:ss:mm:hh:d:MM:yyyy");
		//sdf.setTimeZone(TimeZone.getTimeZone("UTC")); //GMT UTC
		//return( sdf.format(currentTime) );
		return Long.toString( System.currentTimeMillis() );
	}
}
