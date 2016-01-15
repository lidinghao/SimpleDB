package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public static class Table{
        public DbFile getFile() {
            return file;
        }

        public void setFile(DbFile file) {
            this.file = file;
        }

        public String getpKey() {
            return pKey;
        }

        public void setpKey(String pKey) {
            this.pKey = pKey;
        }

        public Table(DbFile file, String pKey) {
            this.file = file;
            this.pKey = pKey;
        }

        private DbFile file;
        private String pKey;

    }


    Map<String,Table> tables ;
    Map<Integer, Table> tablesById;
    public Catalog() {
        tables = new HashMap<>();
        // some code goes here
        tablesById = new HashMap<>();

    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table table = new Table(file, pkeyField);
        tables.put(name, table);
        tablesById.put(file.getId(), table);


    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**e
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        Table t = tables.get(name);
        if (t != null) {
            return t.getFile().getId();

        }else
            throw  new NoSuchElementException("no such table");

        //return 0;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        TupleDesc td = null;
        for(Map.Entry<String,Table> entry:tables.entrySet()){
            if (entry.getValue().getFile().getId() == tableid)
                td = entry.getValue().getFile().getTupleDesc();

        }
        if (td != null) {
            return td;
        }else
            throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        DbFile df = null;
        for(Map.Entry<String,Table> entry:tables.entrySet()){
            if (entry.getValue().getFile().getId() == tableid)
                df = entry.getValue().getFile();

        }
        if (df != null) {
            return df;
        }else
            throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        String pKey = null;
        for(Map.Entry<String,Table> entry:tables.entrySet()){
            if (entry.getValue().getFile().getId() == tableid)
                pKey = entry.getValue().getpKey();

        }
        if (pKey != null) {
            return pKey;
        }else
            throw new NoSuchElementException();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here

        return tablesById.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        for (Map.Entry<String, Table> table : tables.entrySet()) {
            if (table.getValue().getFile().getId() == id) {
                return table.getKey();
            }
        }
        return null;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        tables = new HashMap<>();
        tablesById = new HashMap<>();
        // some code goes here
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String absolutePath=new File(catalogFile).getAbsolutePath();
        String baseFolder=new File(absolutePath).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

