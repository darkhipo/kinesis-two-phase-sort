package com.calamp.services.kinesis.events.utils;

import java.util.Comparator;

public class CalAmpEventPriorityComparator implements Comparator<com.calamp.services.kinesis.events.data.CalAmpEvent>{

	/**Returns a negative integer, zero, or a positive integer as the first argument 
	 * is less than, equal to, or greater than the second.
	 */
	@Override
	public int compare(com.calamp.services.kinesis.events.data.CalAmpEvent e1, com.calamp.services.kinesis.events.data.CalAmpEvent e2){
		int c1 = (int) ( e1.getTimeStamp() - e2.getTimeStamp() );		//Sort by time-stamp 
		int c2 = (int) ( e1.getSequenceNumber() - e2.getSequenceNumber() );			//then by sequence number
		int c3 = (int) ( (e1.getIsAnAck() ? 1 : 0) - (e2.getIsAnAck() ? 1 : 0) );		//then by boolean "is message_type==1" (true has precedence)
		int c4 = (int) ( e1.getMachineId() - e2.getMachineId() );	//then by machine identifier.
		
		if( c1 != 0 ){
			return c1;
		}
		if( c2 != 0 ){
			return c2;
		}
		if( c3 != 0 ){
			return c3;
		}
		if( c4 != 0 ){
			return c4;
		}
		return 0;
	}
}
