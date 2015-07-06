package com.calamp.services.kinesis.events.utils;

import java.util.Comparator;

public class CalAmpEventPriorityComparator implements Comparator<com.calamp.services.kinesis.events.utils.CalAmpEvent>{

	/**Returns a negative integer, zero, or a positive integer as the first argument 
	 * is less than, equal to, or greater than the second.
	 */
	@Override
	public int compare(com.calamp.services.kinesis.events.utils.CalAmpEvent e1, com.calamp.services.kinesis.events.utils.CalAmpEvent e2){
		return (int) ( e1.getMyTime() - e2.getMyTime() );
	}
}
