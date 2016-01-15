package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a fields. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new fields with the specified schema (type).
     * 
     * @param td
     *            the schema of this fields. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    private TupleDesc td;
    private List<Field> fields;
    private RecordId rId;
    public Tuple(TupleDesc td) {
        this.td = td;
        int i =td.numFields();
        this.fields = new ArrayList<Field>(td.numFields());


        // some code goes here
    }

    /**
     * @return The TupleDesc representing the schema of this fields.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this fields on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rId;
    }

    /**
     * Set the RecordId information for this fields.
     * 
     * @param rid
     *            the new RecordId for this fields.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        rId = rid;
    }

    /**
     * Change the value of the ith field of this fields.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (fields.size() > i)
        fields.set(i, f);
        else
            fields.add(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return (Field) fields.get(i);
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
        String str = new  String();
        for (int i = 0; i < fields.size() ; i++) {
            str += fields.get(i).toString();
            if (i == fields.size()-1)
                str+='\n';
            else
                str +='\t';
        }
        return str;

    }
    public static Tuple merge(Tuple t1, Tuple t2){
        TupleDesc td = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple tuple = new Tuple(td);
        tuple.fields.addAll(t1.fields);
        tuple.fields.addAll(t2.fields);
        return tuple;

    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this fields
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fields.iterator();
    }
}
