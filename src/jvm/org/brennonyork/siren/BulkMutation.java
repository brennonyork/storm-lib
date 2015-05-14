package org.brennonyork.siren;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.List;
import java.util.UUID;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;

import org.apache.log4j.Logger;

/**
 * Generates RFiles for bulk ingest into an Accumulo cluster.
 *
 * This class is built to maximize the ingest rate of an Accumulo cluster
 * by leveraging a series of Bulk mutations to generate RFiles which can then
 * be placed into the appropriate Accumulo table through the BulkIngest Bolt.
 * One primary caveat to this Bolt is that it MUST reside with an associated
 * Hadoop instance. Since Storm is distributed in nature this merely requires
 * that the Storm installation also maintain an instance of Hadoop. This mandate
 * is placed because of the decentralized nature of Storm and the necessity for 
 * the equivalent decentralized method call for a file system to write said RFiles
 * to.</br></br>
 *
 * <b>Configuration</b></br>
 * <table>
 *   <tr><td>bulk.output.path</td>
 *       <td>Set the output path deterministically for each RFile generated. This defaults
 *           to the HDFS path hdfs://tmp/rf.</td></tr>
 *   <tr><td>bulk.write.timeout</td>
 *       <td>Set the timeout (in seconds) for any in-memory data structures to be written out
 *           to disk. This defaults to 300 seconds (5 minutes).</td></tr>
 *   <tr><td>bulk.max.records</td>
 *       <td>Set the maximum number of records to maintain in memory before writing out to
 *           disk. This defaults to 1.000.000.000 (1 million).</td></tr>
 * </table>
 * <b>OutputFieldsDeclaration</b></br>
 * <table>
 *   <tr><td>table-name</td>
 *       <td>Corresponds to the table within Accumulo with which to place the RFile into.</td></tr>
 *   <tr><td>abs-rfile-path</td>
 *       <td>The absolute path of the resulting RFile residing in HDFS.</td></tr>
 * </table>
 * 
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York
 */
public class BulkMutation extends BaseRichBolt {
    static Logger log = Logger.getLogger(BulkMutation.class);
	
    OutputCollector _collector;
    Configuration _conf;
    FileSystem _fs;
    FileSKVWriter _writer;
	
    private Map _s;
    private List _rowSchema;
    private List _cfSchema;
    private List _cqSchema;
    private String _cvSchema;
    private String _tsSchema;
    private List _valSchema;
	
    private Text _row;
    private Text _cf;
    private Text _cq;
    private Text _cv;
    private long _ts;
    private Value _val;
    private TreeMap<Key, Value> _recordSet = null;

    private String _TableName = "DefaultTable";
    private String _OutputPath = "/tmp/rf";
    private String _RFilePath;
    private Values nullValue = new Values("");

    private int _maxTimeInterval = 300; // 5 minutes (in seconds)
    private int _maxNumRecords = 1000000000; // 1 million
    private int _currNumRecords = 0; // The current number of records at any given moment

    /**
     * Constructor which takes a single KVschema object to denote how it will operate on
     * a given stream of tuples.
     *
     * @param schema A KVSchema object which represents the Accumulo schema to serialize
     *               the tuples into.
     */
    public BulkMutation(KVSchema schema) {
	_s = schema.serialize();
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	_collector = collector;

	// Handle any instantiated variables passed in through the 'conf' object
	if(conf.containsKey("bulk.output.path")) {
	    _OutputPath = conf.get("bulk.output.path").toString();
	    log.debug("bulk.output.path="+_OutputPath);
	}
	if(conf.containsKey("bulk.write.timeout")) {
	    _maxTimeInterval = Integer.parseInt(conf.get("bulk.write.timeout").toString());
	    log.debug("bulk.write.timeout="+_maxTimeInterval);
	}
	if(conf.containsKey("bulk.max.records")) {
	    _maxNumRecords = Integer.parseInt(conf.get("bulk.max.records").toString());
	    log.debug("bulk.max.records="+_maxNumRecords);
	}

	_TableName = (String)_s.get("table.name");
	_rowSchema = (List)_s.get("row");
	_cfSchema = (List)_s.get("column.family");
	_cqSchema = (List)_s.get("column.qualifier");
	_cvSchema = (String)_s.get("column.visibility");
	_tsSchema = (String)_s.get("timestamp");
	_valSchema = (List)_s.get("value");

	_recordSet = new TreeMap<Key, Value>();
	_conf = new Configuration();

	try {
	    _fs = FileSystem.get(_conf);
	} catch(IOException e) {
	    e.printStackTrace();
	}

	// Ensure a writer can successfully connect and write to the system
	try {
	    _RFilePath = _OutputPath.concat(Path.SEPARATOR).concat(UUID.randomUUID().toString()).concat(".rf");
	    _writer = FileOperations.getInstance().openWriter(_RFilePath, _fs, _conf, AccumuloConfiguration.getDefaultConfiguration());
	    _writer.startDefaultLocalityGroup();
	} catch(IOException e) {
	    e.printStackTrace();
	}

    }

    private static boolean isStaticString(String s) {
	if(s.startsWith("\"") && s.endsWith("\"")) {
	    return true;
	} else {
	    return false;
	}
    }
	
    private void setKVPair(Tuple t) {
	StringBuilder rowSB = new StringBuilder();
	StringBuilder cfSB = new StringBuilder();
	StringBuilder cqSB = new StringBuilder();
	StringBuilder valSB = new StringBuilder();
	String item;

	/// ROW ID
	for(int i = 0; i < _rowSchema.size(); ++i) {
	    item = (String)_rowSchema.get(i);

	    if(isStaticString(item)) {
		rowSB.append(item.substring(1, item.length()-1));
	    } else {
		// If statement here to check whether the field needs metadata augmentation
		rowSB.append(t.getStringByField(item));
	    }
	}
	_row = new Text(rowSB.toString());

	/// COLUMN FAMILY
	if(!(_cfSchema == null) && !(_cfSchema.isEmpty())) {
	    for(int i = 0; i < _cfSchema.size(); ++i) {
		item = (String)_cfSchema.get(i);

		if(isStaticString(item)) {
		    cfSB.append(item.substring(1, item.length()-1));
		} else {
		    // If statement here to check whether the field needs metadata augmentation
		    cfSB.append(t.getStringByField(item));
		}
	    }
	    _cf = new Text(cfSB.toString());
	} else {
	    _cf = new Text();
	}

	/// COLUMN QUALIFIER
	if(!(_cqSchema == null) && !(_cqSchema.isEmpty())) {
	    for(int i = 0; i < _cqSchema.size(); ++i) {
		item = (String)_cqSchema.get(i);

		if(isStaticString(item)) {
		    cqSB.append(item.substring(1, item.length()-1));
		} else {
		    // If statement here to check whether the field needs metadata augmentation
		    cqSB.append(t.getStringByField(item));
		}
	    }
	    _cq = new Text(cqSB.toString());
	} else {
	    _cq = new Text();
	}

	/// COLUMN VISIBILITY
	if(!(_cvSchema == null) && !(_cvSchema.isEmpty())) {
	    _cv = new Text(t.getStringByField(_cvSchema));
	} else {
	    _cv = new Text();
	}
		
	/// TIMESTAMP
	if(!(_tsSchema == null) && !(_tsSchema.isEmpty())) {
	    _ts = Long.parseLong(t.getStringByField(_tsSchema));
	} else {
	    _ts = System.currentTimeMillis();
	}
		
	/// VALUE
	if(!(_valSchema == null) && !(_valSchema.isEmpty())) {
	    for(int i = 0; i < _valSchema.size(); ++i) {
		item = (String)_valSchema.get(i);

		if(isStaticString(item)) {
		    valSB.append(item.substring(1, item.length()-1));
		} else {
		    // If statement here to check whether the field needs metadata augmentation
		    valSB.append(t.getStringByField(item));
		}
	    }
	    _val = new Value(cqSB.toString().getBytes());
	} else {
	    _val = new Value("".getBytes());
	}
    }

    private void transform() {
	for(Entry<Key, Value> kv : _recordSet.entrySet()) {
	    try {
		_writer.append(kv.getKey(), kv.getValue());
	    } catch(IOException e) {
		e.printStackTrace();
	    }
	}
	try {
	    _writer.close();
	} catch(IOException e) {
	    e.printStackTrace();
	}
	_recordSet.clear();
    }

    private void internalTransform() {
	for(Entry<Key, Value> kv : _recordSet.entrySet()) {
	    try {
		_writer.append(kv.getKey(), kv.getValue());
	    } catch(IOException e) {
		e.printStackTrace();
	    }
	}
	try {
	    _writer.close();
	} catch(IOException e) {
	    e.printStackTrace();
	}
	_recordSet.clear();
	try {
	    _RFilePath = _OutputPath.concat(Path.SEPARATOR).concat(UUID.randomUUID().toString()).concat(".rf");
	    _writer = FileOperations.getInstance().openWriter(_RFilePath, _fs, _conf, AccumuloConfiguration.getDefaultConfiguration());
	    _writer.startDefaultLocalityGroup();
	} catch(IOException e) {
	    e.printStackTrace();
	}	
    }

    public void execute(Tuple tuple) {
	setKVPair(tuple);
	_recordSet.put(new Key(_row, _cf, _cq, _cv), new Value(_val));
	_currNumRecords += 1;

	if(_currNumRecords == _maxNumRecords || 
	   ((int)(System.currentTimeMillis() / 1000 /* Convert to seconds */) % _maxTimeInterval) == 0) {
	    log.debug("Writing records");
	    String prevRFilePath = _RFilePath;
	    internalTransform();
	    _currNumRecords = 0;
	    _collector.emit(tuple, new Values(_TableName, prevRFilePath));
	    _collector.ack(tuple);
	}
    }

    public void cleanup() {
	transform();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("table-name", "abs-rfile-path"));
    }

    public Map getComponentConfiguration() {
	return null;
    }
}
