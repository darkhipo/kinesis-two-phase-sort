package com.calamp.services.kinesis.events.utils;

public class EventAgeTest {
	public static boolean oldEnough (com.calamp.services.kinesis.events.utils.Event e){
		return ( (System.currentTimeMillis() - e.getMyTime()) > com.calamp.services.kinesis.events.utils.Parameters.minimumAgeMillis );
	}
}
