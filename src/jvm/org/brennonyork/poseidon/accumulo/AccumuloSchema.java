package org.brennonyork.poseidon.accumulo;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Schema object to denote layout within the Accumulo data store.
 *
 * The AccumuloSchema handles setting all relevant fields within a single Accumulo schema.
 * The premise mandates that a given Schema, to be valid, have a table name with which to
 * place the data in and, at minimum, a single row element.</br></br>
 * 
 * At the end of the day the AccumuloSchema object is just a glorified HashMap with some
 * special qualities. To operate on an AccumuloSchema object each core element has its
 * specific get() method such that a developer can operate on each element. This can take
 * place because of the underlying class which handles internal fields and elements. To
 * create a new schema one can operate on things as such:</br></br>
 *
 * <code>AccumuloSchema kvs = new AccumuloSchema("table_name", "initial_field");</br>
 *       kvs.getRow().add("some_tuple_field").addStatic("NAME").add("another_tuple_field");</br>
 *       kvs.getColumnFamily().addStatic("I AM A COLUMN FAMILY");</br>
 *       kvs.getColumnQualifier().add("colQual_tuple_field");</br>
 *       kvs.getTimestamp() = "tuple_field_to satisfy_timestamp";</br>
 *       kvs.getColumnVisibility() = "tuple_field_to_represent_column_visibility";</br>
 *       kvs.getValue().add("tuple_value").addStatic("\u0000").add("other_value");</br>
 * </code>
 *
 * @author Brennon York
 */
public class AccumuloSchema extends HashMap<String,Object> {
    private static String KV_ELEMENT = "keyvalue.element";
    private static String KV_FIELD = "keyvalue.field";
    public static String KV_TABLE_NAME = "keyvalue.table.name";
    public static String KV_ROW = "keyvalue.row";
    public static String KV_COLUMN_FAMILY = "keyvalue.column.family";
    public static String KV_COLUMN_QUALIFIER = "keyvalue.column.qualifier";
    public static String KV_COLUMN_VISIBILITY = "keyvalue.column.visibility";
    public static String KV_TIMESTAMP = "keyvalue.timestamp";
    public static String KV_VALUE = "keyvalue.value";

    public class KVElement extends HashMap<String,ArrayList<String>> {
	/** Constructor */
	public KVElement() { 
	    this.put(AccumuloSchema.KV_ELEMENT, new ArrayList<String>());
	    this.put(AccumuloSchema.KV_FIELD, new ArrayList<String>());
	}

	/**
	 * Add an item into the KVElement. The string provided must match exactly
	 * to a Field within the incoming Tuple.
	 *
	 * @param item string representing a Field within the incoming Tuple
	 *             whose value will be appended onto the given KVElement
	 */
	public KVElement add(String item) {
	    this.get(AccumuloSchema.KV_ELEMENT).add(item);
	    this.get(AccumuloSchema.KV_FIELD).add(item);
	    return this;
	}

	/**
	 * Add a set of items into the KVElement. The strings provided must match exactly
	 * to Fields within the incoming Tuple.
	 *
	 * @param items List of strings representing Fields within the incoming Tuple
	 *              whose values will be appended onto the given KVElement
	 */
     	public KVElement addAll(List<String> items) {
	    this.get(AccumuloSchema.KV_ELEMENT).addAll(items);
	    this.get(AccumuloSchema.KV_FIELD).addAll(items);
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
	    this.get(AccumuloSchema.KV_ELEMENT).add((new StringBuilder())
						    .append("\"")
						    .append(item)
						    .append("\"")
						    .toString());
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
	    return this.get(AccumuloSchema.KV_ELEMENT).isEmpty();
	}

	/**
	 * Returns the list of elements making up a KVElement.
	 *
	 * @return the list of strings necessary within the Trident stream for this
	 *         KVElement to be fulfilled
	 */
	public List<String> getElements() {
	    return (List)this.get(AccumuloSchema.KV_ELEMENT);
	}
    }

    /**
     * Constructor which mandates a table name for the schema and
     * an initial row element looked up from the incoming Tuple.
     *
     * @param table The table name for the given given to be applied to
     * @param initRow The initial row that must be supplied for a valid Schema
     */
    public AccumuloSchema(String table, String initRow) {
	this.put(AccumuloSchema.KV_TABLE_NAME, table);
	this.put(AccumuloSchema.KV_ROW, new KVElement().add(initRow));
	this.put(AccumuloSchema.KV_COLUMN_FAMILY, new KVElement());
	this.put(AccumuloSchema.KV_COLUMN_QUALIFIER, new KVElement());
	this.put(AccumuloSchema.KV_COLUMN_VISIBILITY, new KVElement());
	this.put(AccumuloSchema.KV_TIMESTAMP, new KVElement());
	this.put(AccumuloSchema.KV_VALUE, new KVElement());
    }

    public static KVElement getRow(Map schema) {
	return (KVElement)schema.get(AccumuloSchema.KV_ROW);
    }

    public KVElement getRow() {
	return getRow(this);
    }

    public static KVElement getColumnFamily(Map schema) {
	return (KVElement)schema.get(AccumuloSchema.KV_COLUMN_FAMILY);
    }

    public KVElement getColumnFamily() {
	return getColumnFamily(this);
    }

    public static KVElement getColumnQualifier(Map schema) {
	return (KVElement)schema.get(AccumuloSchema.KV_COLUMN_QUALIFIER);
    }

    public KVElement getColumnQualifier() {
	return getColumnQualifier(this);
    }

    public static KVElement getColumnVisibility(Map schema) {
	return (KVElement)schema.get(AccumuloSchema.KV_COLUMN_VISIBILITY);
    }

    public KVElement getColumnVisibility() {
	return getColumnVisibility(this);
    }

    public static KVElement getTimestamp(Map schema) {
	return (KVElement)schema.get(AccumuloSchema.KV_TIMESTAMP);
    }

    public KVElement getTimestamp() {
	return getTimestamp(this);
    }

    public static KVElement getValue(Map schema) {
	return (KVElement)schema.get(AccumuloSchema.KV_VALUE);
    }

    public KVElement getValue() {
	return getValue(this);
    }

    public static Fields getFields(AccumuloSchema schema) {
	ArrayList<String> fields = new ArrayList<String>();
	fields.addAll(schema.getRow().getElements());
	fields.addAll(schema.getColumnFamily().getElements());
	fields.addAll(schema.getColumnQualifier().getElements());
	fields.addAll(schema.getColumnVisibility().getElements());
	fields.addAll(schema.getTimestamp().getElements());
	fields.addAll(schema.getValue().getElements());

	return new Fields(fields);
    }

    public Fields getFields() {
	return getFields(this);
    }
}
