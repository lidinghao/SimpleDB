package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new tuple with the specified schema (type).
	 * 
	 * @param td
	 *            the schema of this tuple. It must be a valid TupleDesc
	 *            instance with at least one field.
	 */

	private TupleDesc tDesc;
	private Field[] fields = null;
	private RecordId recordID = null;

	private void Init(TupleDesc td) {
		tDesc = td;
		fields = new Field[tDesc.numFields()];
		recordID = null;
	}

	public Tuple(TupleDesc td) {
		// some code goes here
		Init(td);
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return tDesc;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May
	 *         be null.
	 */
	public RecordId getRecordId() {
		// some code goes here
		return recordID;
	}

	/**
	 * Set the RecordId information for this tuple.
	 * 
	 * @param rid
	 *            the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		// some code goes here
		recordID = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 * 
	 * @param i
	 *            index of the field to change. It must be a valid index.
	 * @param f
	 *            new value for the field.
	 */
	public void setField(int i, Field f) {
		// some code goes here

		fields[i] = f;
	}

	/**
	 * @return the value of the ith field, or null if it has not been set.
	 * 
	 * @param i
	 *            field index to return. Must be a valid index.
	 */
	public Field getField(int i) {
		// some code goes here
		return fields[i];
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the
	 * system tests, the format needs to be as follows:
	 * 
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
	 * 
	 * where \t is any whitespace, except newline, and \n is a newline
	 */
	public String toString() {
		// some code goes here
		// throw new UnsupportedOperationException("Implement this");

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < fields.length; i++) {
			sb.append(fields[i]);
			if (i != fields.length - 1) {
				sb.append("\t");
			}
		}
		sb.append("\n");
		return sb.toString();
	}

	/**
	 * @return An iterator which iterates over all the fields of this tuple
	 */
	public Iterator<Field> fields() {
		// some code goes here

		return (Iterator<Field>) Arrays.asList(fields);
	}

	/**
	 * reset the TupleDesc of thi tuple
	 */
	public void resetTupleDesc(TupleDesc td) {
		// some code goes here
		Init(td);
	}
}
