package org.brennonyork.siren;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Schema object to denote layout in a Key Value data store.
 *
 * The KVSchema handles setting all relevant fields within a Key Value
 * data store. The premise mandates that a given Schema, to be valid,
 * have a table name with which to place the data in and, at minimum, a
 * single row element.</br></br>
 * 
 * To operate on a KVSchema object each core element has been made public
 * such that a developer can operate on each element without necessity of
 * method calls. This can take place because the underlying class (KVElement)
 * handles all mandates on immutability. That said one can access and assign
 * elements in this format:</br></br>
 *
 * <code>KVSchema kvs = new KVSchema("table_name", "initial_field");</br>
 *       kvs.row.add("some_tuple_field").addStatic("NAME").add("another_tuple_field");</br>
 *       kvs.colFam.addStatic("I AM A COLUMN FAMILY");</br>
 *       kvs.colQual.add("colQual_tuple_field");</br>
 *       kvs.timestamp = "tuple_field_to satisfy_timestamp";</br>
 *       kvs.colVis = "tuple_field_to_represent_column_visibility";</br>
 *       kvs.value.add("tuple_value").addStatic("\u0000").add("other_value");</br>
 * </code>
 *
 * @author Brennon York
 */
public class KVSchema {
    /**
     * Single element within a Key Value Schema which handles the 
     * immutable mandate on the internal List.
     *
     * @author Brennon York
     */
    public class KVElement {
	/** Local list containing all declared fields for a given Key Value Element */
	private List<String> _al;
	/** Local declaration of fields for a given KVElement */
	private List<String> _fields;

	/** Constructor */
	public KVElement() { 
	    _al = new ArrayList<String>();
	    _fields = new ArrayList<String>();
	}

	/**
	 * Add an item into the KVElement. The string provided must match exactly
	 * to a Field within the incoming Tuple.
	 *
	 * @param item string representing a Field within the incoming Tuple
	 *             whose value will be appended onto the given KVElement
	 * @return this KVSchema object
	 */
	public KVElement add(String item) {
	    _al.add(item);
	    _fields.add(item);
	    return this;
	}

	/**
	 * Adds a static item to the KVElement. This is useful for delimiters
	 * as well as static naming conventions.</br></br>
	 *
	 * For example:</br>
	 * <code>KVSchema kvs = new KVSchema("table", "field").row.addStatic("\u0000")</code></br>
	 * will add a static null byte after the "field" element within the row.
	 *
	 * @param item static string to insert into the KVElement
	 * @return this KVSchema object
	 */
	public KVElement addStatic(String item) {
	    _al.add((new StringBuilder()).append("\"").append(item).append("\"").toString());
	    return this;
	}

	/**
	 * Determines whether the ordered List maintaining the KVElement is
	 * empty or not.
	 *
	 * @return boolean value indicating whether the given KVElement object
	 *         is empty
	 */
	public boolean isEmpty() {
	    return _al.isEmpty();
	}

	/**
	 * Serializes the KVElement returning an ordered List of fields necessary
	 * to represent the KVElement within the data store.
	 *
	 * @return List object that represents the individual KVElement
	 */
	public List serialize() {
	    return _al;
	}

	/**
	 * Serializes all necessary fields into a List object such that the outer KVSchema
	 * can consolidate tuple fields for schema completion.
	 *
	 * @return List object representing all necessary fields to complete the KVElement
	 */
	public List getFields() {
	    return _fields;
	}
    }

    public static String KV_TABLE_NAME = "table.name";
    public static String KV_ROW = "row";
    public static String KV_COLUMN_FAMILY = "column.family";
    public static String KV_COLUMN_QUALIFIER = "column.qualifier";
    public static String KV_COLUMN_VISIBILITY = "column.visibility";
    public static String KV_TIMESTAMP = "timestamp";
    public static String KV_VALUE = "value";    

    /** Represents the row within the data store as a KVElement */
    public KVElement row;
    /** Represents the column family within the data store as a KVElement */
    public KVElement colFam;
    /** Represents the column qualifier within the data store as a KVElement */
    public KVElement colQual;
    /** Represents the column visibility within the data store as a String */
    public String colVis;
    /** Represents the timestamp within the data store as a String */
    public String timestamp;
    /** Represents the value within the data store as a KVElement */
    public KVElement value;
    /** The table where this schema will be placed into */
    private String _table;
    /** Internal data structure maintaining the schema */
    private Map _schema = new HashMap();
    /** Internal structure to return all relevant fields for the KVSchema */
    private List<String> _fields;

    /**
     * Constructor which mandates a table name for the schema and
     * an initial row element looked up from the incoming Tuple.
     *
     * @param table The table name for the given given to be applied to
     * @param initRow The initial row that must be supplied for a valid Schema
     */
    public KVSchema(String table, String initRow) {
	_table = table;

	row = new KVElement();
	row.add(initRow);
	colFam = new KVElement();
	colQual = new KVElement();
	colVis = "";
	timestamp = "";
	value = new KVElement();
    }

    public Fields getFields() {
	_fields = new ArrayList<String>(row.getFields());

	if(!colFam.isEmpty()) {
            _fields.addAll(colFam.getFields());
	}

        if(!colQual.isEmpty()) {
            _fields.addAll(colQual.getFields());
	}

        if(!colVis.isEmpty()) {
            _fields.add(colVis);
	}

        if(!timestamp.isEmpty()) {
            _fields.add(timestamp);
	}

        if(!value.isEmpty()) {
            _fields.addAll(value.getFields());
	}

	return new Fields(_fields);
    }

    /**
     * Returns a nested Map structure representing the KVSchema which
     * can be passed through the Storm stream, if necessary.
     *
     * @return map structure representing the given KVSchema object
     */
    public Map serialize() {
	_schema.put("table.name", _table);
	_schema.put("row", row.serialize());
		
	if(!colFam.isEmpty()) {
	    _schema.put("column.family", colFam.serialize());
	}
		
	if(!colQual.isEmpty()) {
	    _schema.put("column.qualifier", colQual.serialize());
	}
		
	if(!colVis.isEmpty()) {
	    _schema.put("column.visibility", colVis);
	}
		
	if(!timestamp.isEmpty()) {
	    _schema.put("timestamp", timestamp);
	}
		
	if(!value.isEmpty()) {
	    _schema.put("value", value.serialize());
	}
		
	return _schema;
    }
}
