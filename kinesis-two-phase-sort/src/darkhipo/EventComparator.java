package darkhipo;

import java.util.Comparator;

public class EventComparator implements Comparator<darkhipo.Event>{

	/**Returns a negative integer, zero, or a positive integer as the first argument 
	 * is less than, equal to, or greater than the second.
	 */
	@Override
	public int compare(darkhipo.Event e1, darkhipo.Event e2){
		return (int) ( e1.getMyTime() - e2.getMyTime() );
	}
}
