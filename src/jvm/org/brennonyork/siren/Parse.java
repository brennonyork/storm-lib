package org.brennonyork.siren;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Formatter;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;
import org.brennonyork.siren.Functor;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Handles parsing of a RecordTemplate object into its constituent Storm fields.
 *
 * This class is meant to abstract out parsing of basic files and types into a Storm
 * stream. It reads in files from a resident HDFS instance and parses those files in
 * the manner described within a RecordTemplate object. All objects emitted into the
 * stream are a series of strings as byte sequences. If it reads strings it will output
 * the string with its corresponding field label as provided within the RecordTemplate.
 * If it reads a series of bytes it will output them as their string-ified hexidecimal
 * equalivalent. Concretely, if bytes <code>0x00,0x4f,0xed</code> are read into the stream
 * for a given field, their output tuple field would contain the information 
 * <code>004FED</code> and it is up to the topology developer to convert that string back
 * into its series of bytes. To get data parsed by this object it will read the first item
 * from the stream as a string and assume it is a complete HDFS path to a resulting file.</br></br>
 *
 * <b>OutputFieldsDeclaration</b></br>
 * <table>
 *   <tr><td><i>dynamic</i></td>
 *       <td>All fields are declared through the RecordTemplate object that is provided
 *           upon construction of this object. As such the actual declaration of fields is
 *           generated dynamically from the information passed through said 
 *           RecordTemplate.</td></tr>
 * </table>
 * 
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York
 */
public class Parse extends BaseRichBolt {
    static Logger log = Logger.getLogger(Parse.class);

    private static final byte[] HEX_CHAR = new byte[]
	{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    private OutputCollector _collector;
    private Fields _outputFields;
    private InputStream _is;
    private ArrayList<ArrayList<String>> _recordTemplate;

    public Parse(RecordTemplate rt) {
	_recordTemplate = (ArrayList<ArrayList<String>>)rt.serialize();
	_outputFields = rt.getFields();
    }

    public static String asHex(byte[] buf)
    {
        char[] chars = new char[2 * buf.length];
        for (int i = 0; i < buf.length; ++i)
	    {
		chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
		chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
	    }
        return new String(chars);
    }

    public String readString(InputStream is, String delim) {
	StringBuffer sb = new StringBuffer();
	try {
	    while(sb.append((char)is.read()).lastIndexOf(delim) == -1) { }
	} catch(IOException e) {
	    e.printStackTrace();
	}
	sb.deleteCharAt(sb.length() - 1);
	return sb.toString().replaceAll("[^\\x00-\\x7F]", "");
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	_collector = collector;
    }

    public void execute(Tuple tuple) {
	try {
	    _is = new BufferedInputStream(new FileInputStream(new File(tuple.getString(0))));
	} catch (FileNotFoundException e) {
	    log.error("Could not open file "+tuple.getString(0));
	}

	ArrayList<String> _field;
	Values _v;

	try {
	    _is.mark(1);
	    while(_is.read() != -1) {
		_is.reset();
		_v = new Values();
		
		// Parse record
		for(int i = 0; i < _recordTemplate.size(); ++i) {
		    _field = (ArrayList<String>)_recordTemplate.get(i);
		    int size = _field.size();
		    String ident = (String)_field.get(0);

		    if(ident.equals("I")) {
			int byteNum = Integer.parseInt((String)_field.get(1));

			if(size == 2) {
			    _is.skip(byteNum);
			} else if(size == 3) {
			    byte[] ba = new byte[byteNum];
			    _is.read(ba, 0, byteNum);
			    _v.add(asHex(ba));
			} else {
			    // TODO: Handle error 
			}
		    } else if(ident.equals("S")) {
			if(size == 2) {
			    readString(_is, (String)_field.get(1));
			} else if(size == 3) {
			    _v.add(readString(_is, (String)_field.get(1)));
			} else {
			    // TODO: Handle error 
			}
		    } else {
			// TODO: Handle error
		    }
		}

		_collector.emit(tuple, _v);
		_collector.ack(tuple);

		_is.mark(1);
	    }
	} catch (IOException e) {
	    log.error("Error while parsing file "+tuple.getString(0));
	    e.printStackTrace();
	}
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(_outputFields);
    }

    public Map getComponentConfiguration() {
	return null;
    }
}
