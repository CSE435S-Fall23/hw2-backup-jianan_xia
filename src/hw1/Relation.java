package hw1;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
//		return null;
		 ArrayList<Tuple> selectedTuples = new ArrayList<>();
	        for (Tuple tuple : tuples) {
	            if (tuple.getField(field).compare(op, operand)) {
	                selectedTuples.add(tuple);
	            }
	        }
	        return new Relation(selectedTuples, td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
//		return null;
		 String[] newFieldNames = new String[td.numFields()];
	        for (int i = 0; i < td.numFields(); i++) {
	            if (fields.contains(i)) {
	                int index = fields.indexOf(i);
	                newFieldNames[i] = names.get(index);
	            } else {
	                newFieldNames[i] = td.getFieldName(i);
	            }
	        }
	        Type[] newFieldTypes = new Type[td.numFields()];
	        for (int i = 0; i < td.numFields(); i++) {
	            newFieldTypes[i] = td.getType(i);
	        }
	        TupleDesc newTd = new TupleDesc(newFieldTypes, newFieldNames);

	        ArrayList<Tuple> renamedTuples = new ArrayList<>();
	        for (Tuple tuple : tuples) {
	            Field[] newFields = new Field[td.numFields()];
	            for (int i = 0; i < td.numFields(); i++) {
	                newFields[i] = tuple.getField(i);
	            }
	            Tuple newTuple = new Tuple(newTd);
	            for (int i = 0; i < td.numFields(); i++) {
	                newTuple.setField(i, newFields[i]);
	            }
	            renamedTuples.add(newTuple);
	        }

	        return new Relation(renamedTuples, newTd);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
//		return null;
		Type[] projectedTypes = new Type[fields.size()];
	    String[] projectedFields = new String[fields.size()];

	    for (int i = 0; i < fields.size(); i++) {
	        int fieldIndex = fields.get(i);
	        if (fieldIndex < 0 || fieldIndex >= this.td.numFields()) {
	            throw new IllegalArgumentException("Invalid field index: " + fieldIndex);
	        }
	        projectedTypes[i] = this.td.getType(fieldIndex);
	        projectedFields[i] = this.td.getFieldName(fieldIndex);
	    }

	    TupleDesc projectedTd = new TupleDesc(projectedTypes, projectedFields);

	    // Create a list to store the projected tuples
	    ArrayList<Tuple> projectedTuples = new ArrayList<>();

	    // Iterate over the tuples in the current relation
	    for (Tuple tuple : this.tuples) {
	        // Create a new Tuple with the projected TupleDesc
	        Tuple projectedTuple = new Tuple(projectedTd);

	        // Copy the selected fields from the current tuple to the projected tuple
	        for (int i = 0; i < fields.size(); i++) {
	            int fieldIndex = fields.get(i);
	            Field field = tuple.getField(fieldIndex);
	            projectedTuple.setField(i, field);
	        }

	        // Add the projected tuple to the result
	        projectedTuples.add(projectedTuple);
	    }

	    // Create a new Relation using the projected tuples and TupleDesc
	    return new Relation(projectedTuples, projectedTd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
//		return null;
		ArrayList<Tuple> joinedTuples = new ArrayList<>();
		Type[] mergedTypes = new Type[this.td.numFields() + other.td.numFields()];
	    String[] mergedFields = new String[this.td.numFields() + other.td.numFields()];

	    for (int i = 0; i < this.td.numFields(); i++) {
	        mergedTypes[i] = this.td.getType(i);
	        mergedFields[i] = this.td.getFieldName(i);
	    }

	    for (int i = 0; i < other.td.numFields(); i++) {
	        mergedTypes[i + this.td.numFields()] = other.td.getType(i);
	        mergedFields[i + this.td.numFields()] = other.td.getFieldName(i);
	    }

	    TupleDesc mergedTd = new TupleDesc(mergedTypes, mergedFields);

	    // Iterate over the tuples in this Relation
	    for (Tuple tuple1 : this.tuples) {
	        // Get the value of the field to be compared in this Relation
	        Field fieldValue1 = tuple1.getField(field1);

	        // Iterate over the tuples in the other Relation
	        for (Tuple tuple2 : other.tuples) {
	            // Get the value of the field to be compared in the other Relation
	            Field fieldValue2 = tuple2.getField(field2);

	            // Check if the values match
	            if (fieldValue1.compare(RelationalOperator.EQ, fieldValue2)) {
	                // Create a new Tuple with the merged TupleDesc
	                Tuple joinedTuple = new Tuple(mergedTd);

	                // Copy fields from tuple1 to the new Tuple
	                for (int i = 0; i < this.td.numFields(); i++) {
	                    joinedTuple.setField(i, tuple1.getField(i));
	                }

	                // Copy fields from tuple2 to the new Tuple
	                for (int i = 0; i < other.td.numFields(); i++) {
	                    joinedTuple.setField(i + this.td.numFields(), tuple2.getField(i));
	                }

	                // Add the joined Tuple to the result
	                joinedTuples.add(joinedTuple);
	            }
	        }
	    }

	    // Create a new Relation using the joined Tuples and the merged TupleDesc
	    return new Relation(joinedTuples, mergedTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
//		return null;
		if (groupBy) {
	        HashMap<Field, Integer> aggregatedData = new HashMap<>();
	        Type[] aggregatedTypes = new Type[2];
	        String[] aggregatedFields = new String[2];
	        aggregatedTypes[0] = this.td.getType(0);
	        aggregatedTypes[1] = Type.INT;
	        aggregatedFields[0] = this.td.getFieldName(0);
	        aggregatedFields[1] = op.toString();

	        TupleDesc aggregatedTd = new TupleDesc(aggregatedTypes, aggregatedFields);

	        ArrayList<Tuple> resultTuples = new ArrayList<>();

	        for (Tuple tuple : this.tuples) {
	            Field groupField = tuple.getField(0);
	            int value = ((IntField) tuple.getField(1)).getValue();

	            if (!aggregatedData.containsKey(groupField)) {
	                aggregatedData.put(groupField, value);
	            } else {
	                int current = aggregatedData.get(groupField);
	                if (op == AggregateOperator.SUM) {
	                    aggregatedData.put(groupField, current + value);
	                } else if (op == AggregateOperator.COUNT) {
	                    aggregatedData.put(groupField, current + 1);
	                } else if (op == AggregateOperator.MAX) {
	                    if (value > current) {
	                        aggregatedData.put(groupField, value);
	                    }
	                } else if (op == AggregateOperator.MIN){
	                    if (value < current) {
	                        aggregatedData.put(groupField, value);
	                    }
	                } 
	            }
	        }
	        
	        if (op == AggregateOperator.AVG) {
	            for (Field groupField : aggregatedData.keySet()) {
	                int total = aggregatedData.get(groupField);
	                int count = 0; 
	                for (Tuple tuple : this.tuples) {
	                    Field currentGroupField = tuple.getField(0);
	                    if (currentGroupField.equals(groupField)) {
	                        count++;
	                    }
	                }
	                aggregatedData.put(groupField, total / count);
	            }
	        }

	        for (Field groupField : aggregatedData.keySet()) {
	            int value = aggregatedData.get(groupField);
	            Tuple resultTuple = new Tuple(aggregatedTd);
	            resultTuple.setField(0, groupField);
	            resultTuple.setField(1, new IntField(value));
	            resultTuples.add(resultTuple);
	        }

	        return new Relation(resultTuples, aggregatedTd);
	    } else {
	        Type[] aggregatedTypes = new Type[1];
	        String[] aggregatedFields = new String[1];
	        aggregatedTypes[0] = Type.INT;
	        aggregatedFields[0] = op.toString();

	        TupleDesc aggregatedTd = new TupleDesc(aggregatedTypes, aggregatedFields);

	        ArrayList<Tuple> resultTuples = new ArrayList<>();

	        if (op == AggregateOperator.COUNT) {
	            int count = this.tuples.size();
	            Tuple resultTuple = new Tuple(aggregatedTd);
	            resultTuple.setField(0, new IntField(count));
	            resultTuples.add(resultTuple);
	            
	        } else if (op == AggregateOperator.SUM) {
	            int sum = 0;
	            for (Tuple tuple : this.tuples) {
	                int value = ((IntField) tuple.getField(0)).getValue();
	                sum += value;
	            }
	            Tuple resultTuple = new Tuple(aggregatedTd);
	            resultTuple.setField(0, new IntField(sum));
	            resultTuples.add(resultTuple);
	            
	        } else if (op == AggregateOperator.MAX) {
	        	int resultValue = Integer.MIN_VALUE;
	        	for (Tuple tuple : this.tuples) {
	                int value = ((IntField) tuple.getField(0)).getValue();
	                if (value > resultValue) {
	                    resultValue = value;
	                }
	            }
	        	Tuple resultTuple = new Tuple(aggregatedTd);
	            resultTuple.setField(0, new IntField(resultValue));
	            resultTuples.add(resultTuple);
	            
	        } else if (op == AggregateOperator.MIN) {
	        	int resultValue = Integer.MAX_VALUE;
	        	for (Tuple tuple : this.tuples) {
	                int value = ((IntField) tuple.getField(0)).getValue();
	                if (value > resultValue) {
	                    resultValue = value;
	                }
	            }
	        	Tuple resultTuple = new Tuple(aggregatedTd);
	            resultTuple.setField(0, new IntField(resultValue));
	            resultTuples.add(resultTuple);
	            
	        } else if (op == AggregateOperator.AVG) {
	        	int sum = 0;
	            for (Tuple tuple : this.tuples) {
	                int value = ((IntField) tuple.getField(0)).getValue();
	                sum += value;
	            }
	            int resultValue = sum / this.tuples.size();
	            Tuple resultTuple = new Tuple(aggregatedTd);
	            resultTuple.setField(0, new IntField(resultValue));
	            resultTuples.add(resultTuple);
	        }

	        return new Relation(resultTuples, aggregatedTd);
	    }
	}
	
	public TupleDesc getDesc() {
		//your code here
//		return null;
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
//		return null;
		 return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
//		return null;
		StringBuilder sb = new StringBuilder();
	    
	    // Append the TupleDesc
	    sb.append("TupleDesc: ");
	    sb.append(this.td.toString());
	    sb.append("\n");
	    
	    // Append each Tuple
	    sb.append("Tuples:\n");
	    for (Tuple tuple : this.tuples) {
	        sb.append(tuple.toString());
	        sb.append("\n");
	    }
	    
	    return sb.toString();
	}
}
