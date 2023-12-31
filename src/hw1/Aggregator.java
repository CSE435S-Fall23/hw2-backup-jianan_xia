// Please see aggregation in Relation class. I put the functions there. 
// And professor said it is okay to write codes there and leave a note here as a reminder for grading.
// Refer to piazza post @105, I asked professor in class and closed that post myself.

package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	private AggregateOperator operator;
	private boolean group;
	private TupleDesc td;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		operator = o;
		group = groupBy;
		this.td = td;
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		return null;
	}

}