package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	/**
	 * A help class to facilitate organizing the information of each field
	 */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 */
		public final Type fieldType;

		/**
		 * The name of the field
		 */
		public final String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	private ArrayList<TDItem> TDs = null;
	private int tupleSize = 0;

	/**
	 * @return An iterator which iterates over all the field TDItems that are
	 *         included in this TupleDesc
	 */
	public Iterator<TDItem> iterator() {
		// some code goes here
		if (TDs == null)
			return null;
		else
			return TDs.iterator();
	}

	private static final long serialVersionUID = 1L;

	private void constructTupleDesc(Type[] typeAr, String[] fieldAr) {
		TDs = new ArrayList<TDItem>();
		tupleSize = 0;
		for (int i = 0; i < typeAr.length; i++) {
			String fieldName = "";
			if (fieldAr != null) {
				fieldName = fieldAr[i];
			}
			TDs.add(new TDItem(typeAr[i], fieldName));
			tupleSize += typeAr[i].getLen();
		}
	}

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. Note that names may
	 *            be null.
	 */

	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		// some code goes here
		constructTupleDesc(typeAr, fieldAr);
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		// some code goes here
		constructTupleDesc(typeAr, null);
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// some code goes here
		return TDs.size();
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 * 
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		// some code goes here
		return TDs.get(i).fieldName;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 * 
	 * @param i
	 *            The index of the field to get the type of. It must be a valid
	 *            index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		// some code goes here
		return TDs.get(i).fieldType;
	}

	/**
	 * Find the index of the field with a given name.
	 * 
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		// some code goes here
		int index = -1;
		for (int i = 0; i < TDs.size(); i++) {
			if (getFieldName(i).equals(name)) {
				index = i;
				break;
			}
		}
		if (index == -1) {
			throw new NoSuchElementException();
		}
		return index;
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// some code goes here
		return tupleSize;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		// some code goes here

		int lenTD1 = td1.numFields();
		int lenTD2 = td2.numFields();
		int len = lenTD1 + lenTD2;

		Type[] typeAr = new Type[len];
		String[] fieldAr = new String[len];

		for (int i = 0; i < td1.numFields(); i++) {
			typeAr[i] = td1.getFieldType(i);
			fieldAr[i] = td1.getFieldName(i);
		}

		for (int i = 0; i < td2.numFields(); i++) {
			typeAr[i + lenTD1] = td2.getFieldType(i);
			fieldAr[i + lenTD1] = td2.getFieldName(i);
		}

		return new TupleDesc(typeAr, fieldAr);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 * 
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {

		TupleDesc that = null;

		if (o == null) {
			return false;
		}

		try {
			that = (TupleDesc) o;
		}

		catch (ClassCastException exp) {
			return false;
		}

		if (numFields() != that.numFields()) {
			return false;
		}

		for (int i = 0; i < numFields(); i++) {
			if (this.getFieldType(i).compareTo(that.getFieldType(i)) != 0) {
				return false;
			}
		}

		return true;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here

		int len = numFields();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(getFieldType(i) + "(" + getFieldName(i) + ")");
		}
		return sb.toString();
	}
}
