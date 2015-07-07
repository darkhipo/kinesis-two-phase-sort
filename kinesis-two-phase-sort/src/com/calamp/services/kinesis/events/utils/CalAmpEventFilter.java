package com.calamp.services.kinesis.events.utils;

import com.calamp.services.kinesis.events.data.CalAmpEvent;

public class CalAmpEventFilter {
	public static boolean oldEnough(CalAmpEvent e){
		return ( (System.currentTimeMillis() - e.getTimeStamp() ) > CalAmpParameters.minimumAgeMillis );
	}
}
