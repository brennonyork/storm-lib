package org.brennonyork.siren;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines how a single record is parsed from a given file.
 *
 * The RecordTemplate defines a concise set of parsing abractions
 * to define a single record from a muli-record file. It works in tandem with
 * the Parse Bolt to generate the necessary Storm fields from the record.
 *
 * @author Brennon York
 */
public class RecordTemplate {
    /** Data structure to maintain the record template */
    private ArrayList<ArrayList<String>> _parse;
    /** Structure which maintains fields for the Storm FieldsDeclaration */
    private List<String> _fields;

    /** Constructor */
    public RecordTemplate() { 
	_parse = new ArrayList<ArrayList<String>>();
	_fields = new ArrayList<String>();
    }
    
    /**
     * Tells the parsing object to skip a set number of bytes.
     *
     * @param byteNum number of bytes to skip from the file
     * @return this RecordTemplate object
     */
    public RecordTemplate skipBytes(int byteNum) {
	ArrayList<String> al = new ArrayList<String>();
	al.add("I");
	al.add(Integer.toString(byteNum));
	_parse.add(al);
	return this;
    }

    /**
     * Tells the parsing object to read <code>byteNum</code> bytes and
     * store them in field labeled <code>fieldName</code>.
     *
     * @param byteNum number of bytes to read from the file
     * @param fieldName tuple field name to emit into the Storm stream
     * @return this RecordTemplate object
     */
    public RecordTemplate readBytes(int byteNum, String fieldName) {
	ArrayList<String> al = new ArrayList<String>();
	al.add("I");
	al.add(Integer.toString(byteNum));
	al.add(fieldName);
	_fields.add(fieldName);
	_parse.add(al);
	return this;
    }

    /**
     * Tells the parsing object to skip a set number of bytes up to a
     * specific delimiting byte <code>delim</code> or set of bytes. If a
     * set of bytes is given (a String) it will look for the exact pattern
     * and will continue to skip until it sees that series of bytes.
     *
     * @param delim string delimiter to read up to
     * @return this RecordTemplate object
     */
    public RecordTemplate skipString(String delim) {
	ArrayList<String> al = new ArrayList<String>();
	al.add("S");
	al.add(delim);
	_parse.add(al);
	return this;
    }

    /**
     * Tells the parsing object to read a set number of bytes up to a
     * specific delimiting byte <code>delim</code> or set of bytes and store
     * that into the field <code>fieldName</code>. If a set of bytes is given 
     * (a String) it will look for the exact pattern and store all bytes up to
     * the final byte, which it will consider the delimiter and remove.
     *
     * @param delim string delimiter to read up to; if multiple characters are 
     *              present, it will match the entire string and store all but
     *              the last character
     * @param fieldName tuple field name to emit into the Storm stream
     * @return this RecordTemplate object
     */
    public RecordTemplate readString(String delim, String fieldName) {
	ArrayList<String> al = new ArrayList<String>();
	al.add("S");
	al.add(delim);
	al.add(fieldName);
	_fields.add(fieldName);
	_parse.add(al);
	return this;
    }

    /**
     * @return data structure representing a given RecordTemplate object
     */
    public List serialize() {
	return _parse;
    }

    /**
     * Returns the set of Storm-specific fields related to the given
     * RecordTemplate at the time of the call.
     *
     * @return Storm Fields object representing all labeled fields captured
     *         from a given RecordTemplate
     */
    public Fields getFields() {
	return new Fields(_fields);
    }
}
