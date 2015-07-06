package com.calamp.services.kinesis.events.utils;

public class CalAmpEventFilter {
	public static boolean oldEnough(CalAmpEvent e){
		return ( (System.currentTimeMillis() - e.getMyTime()) > CalAmpParameters.minimumAgeMillis );
	}
}
