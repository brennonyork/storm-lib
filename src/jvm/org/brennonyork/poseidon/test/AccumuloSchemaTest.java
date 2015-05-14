package org.brennonyork.poseidon.accumulo.test;

import org.brennonyork.poseidon.accumulo.AccumuloSchema;

public class AccumuloSchemaTest {
    public static void main(String[] args) {
	AccumuloSchema kvs = new AccumuloSchema("table_name", "initial_field");
	kvs.getRow().add("some_tuple_field").addStatic("NAME").add("another_tuple_field");
	kvs.getColumnFamily().addStatic("I AM A COLUMN FAMILY");
	kvs.getColumnQualifier().add("colQual_tuple_field");
	//kvs.getColumnVisibility() = "tuple_field_to_represent_column_visibility";
	//kvs.getTimestamp() = "tuple_field_to satisfy_timestamp";
	kvs.getValue().add("tuple_value").addStatic("\u0000").add("other_value");
    }
}
