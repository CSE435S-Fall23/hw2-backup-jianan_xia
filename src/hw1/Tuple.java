package hw1;

import java.sql.Types;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	 private TupleDesc tupleDesc; 
	 private Field[] fields;
	 private int pid;
	 private int id;
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		//your code here
		tupleDesc = t;
        fields = new Field[t.numFields()];
	}
	
	public TupleDesc getDesc() {
		//your code here
		return tupleDesc;
//		return null;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return pid;
//		return 0;
	}

	public void setPid(int pid) {
		//your code here
		this.pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return id;
//		return 0;
	}

	public void setId(int id) {
		//your code here
		this.pid = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		tupleDesc = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		//your code here
		fields[i] = v;
	}
	
	public Field getField(int i) {
		//your code here
		return fields[i];
//		return null;
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            sb.append(fields[i].toString());
            if (i < fields.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
//		return "";
	}
}
	