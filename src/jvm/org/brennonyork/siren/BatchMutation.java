package org.brennonyork.siren;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;

/**
 * Handles direct mutations on a table within an Accumulo instance from 
 * emitted Storm Tuples.
 *
 * The BatchMutation class leverages a KVSchema object for defining how data
 * will reside within an Accumulo instance. It will turn each input tuple
 * into a series of Key Value mutations which are directly inserted into the
 * given table.</br></br>
 *
 * <b>Configuration</b></br>
 * <table>
 *   <tr><td>zookeeper.instance.name</td>
 *       <td>The name of the ZooKeeper instance managing the Accumulo cluster instance.</td></tr>
 *   <tr><td>zookeeper.servers</td>
 *       <td>The comma delimited set of ZooKeeper hostnames associated with the ZooKeeper instance.
 *           For a multi-node instance one would assign the values as:</br>
 *           <code>Map conf = new HashMap();</br>
 *                 conf.put("zookeeper.servers", "zoo.node01,zoo.node02,zoo.node03");</br></code>
 *           </td></tr>
 *   <tr><td>accumulo.user</td>
 *       <td>The name of the Accumulo user when connecting to the database.</td></tr>
 *   <tr><td>accumulo.passwd</td>
 *       <td>The password for the given Accumulo user.</td></tr>
 * </table>
 * <b>OutputFieldsDeclaration</b></br>
 * <table>
 *   <tr><td>Null</td>
 *       <td>Because all entries are inserted into Accumulo through mutations there
 *           are no tuples left to output.</td></tr>
 * </table>
 * 
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York
 */
public class BatchMutation extends BaseRichBolt {
    static Logger log = Logger.getLogger(BatchMutation.class);
	
    private OutputCollector _collector;
    private Connector _conn;
    private BatchWriter _writer;
    private boolean _debug = false;
	
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

    private String _zkInstName = "myinstance";
    private String _zkServers = "localhost";
    private String _AccumuloUser = "root";
    private String _AccumuloPasswd = "passwd";
    private String _TableName = "DefaultTable";
    private long _MemoryBuffer = (1024L * 1024L * 32L); // bytes to store before sending a batch
    private long _timeout = 3000 + (new Random().nextLong() % 7000L); // milliseconds to wait before sending

    private long _timestamp;
    private int _NumThreads = 10;

    public BatchMutation(KVSchema schema) {
	_s = schema.serialize();
    }

    public BatchMutation(KVSchema schema, boolean debug) {
	_s = schema.serialize();
	_debug = debug;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	_collector = collector;
	_TableName = (String)_s.get("table.name");
	_rowSchema = (List)_s.get("row");
	_cfSchema = (List)_s.get("column.family");
	_cqSchema = (List)_s.get("column.qualifier");
	_cvSchema = (String)_s.get("column.visibility");
	_tsSchema = (String)_s.get("timestamp");
	_valSchema = (List)_s.get("value");

	// Handle any instantiated variables passed in through the 'conf' object
	if(conf.containsKey("zookeeper.instance.name")) {
	    _zkInstName = conf.get("zookeeper.instance.name").toString();
	    log.debug("zookeeper.instance.name="+_zkInstName);
	}
	if(conf.containsKey("zookeeper.servers")) {
	    _zkServers = conf.get("zookeeper.servers").toString();
	    log.debug("zookeeper.servers="+_zkServers);
	}
	if(conf.containsKey("accumulo.user")) {
	    _AccumuloUser = conf.get("accumulo.user").toString();
	    log.debug("accumulo.user="+_AccumuloUser);
	}
	if(conf.containsKey("accumulo.passwd")) {
	    _AccumuloPasswd = conf.get("accumulo.passwd").toString();
	    log.debug("accumulo.passwd=found");
	}
	
	if(_debug) {
	    try {
		_conn = new MockInstance(_zkInstName).getConnector("root", "".getBytes());
	    } catch(Exception e) {
		e.printStackTrace();
	    }
	} else {
	    try {
		_conn = new ZooKeeperInstance(_zkInstName, _zkServers).getConnector(_AccumuloUser, _AccumuloPasswd);
	    } catch(Exception e) {
		e.printStackTrace();
	    }
	}

	if(!_conn.tableOperations().exists(_TableName)) {
	    try {
		_conn.tableOperations().create(_TableName);
	    } catch(Exception e) {
		e.printStackTrace();
	    }
	}

	try {
	    _writer = _conn.createBatchWriter(_TableName, _MemoryBuffer, _timeout, _NumThreads);	
	} catch(Exception e) {
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
	StringBuilder rowSB = new StringBuilder(_rowSchema.size());
	String item;

	/// ROW ID
	for(int i = 0; i < _rowSchema.size(); ++i) {
	    item = (String)_rowSchema.get(i);

	    if(isStaticString(item)) {
		rowSB.append(item.substring(1, item.length()-1));
	    } else {
		rowSB.append(t.getStringByField(item));
	    }
	}
	_row = new Text(rowSB.toString());

	/// COLUMN FAMILY
	if(!(_cfSchema == null) && !(_cfSchema.isEmpty())) {
	    StringBuilder cfSB = new StringBuilder(_cfSchema.size());

	    for(int i = 0; i < _cfSchema.size(); ++i) {
		item = (String)_cfSchema.get(i);

		if(isStaticString(item)) {
		    cfSB.append(item.substring(1, item.length()-1));
		} else {
		    cfSB.append(t.getStringByField(item));
		}
	    }
	    _cf = new Text(cfSB.toString());
	} else {
	    _cf = new Text();
	}

	/// COLUMN QUALIFIER
	if(!(_cqSchema == null) && !(_cqSchema.isEmpty())) {
	    StringBuilder cqSB = new StringBuilder(_cqSchema.size());

	    for(int i = 0; i < _cqSchema.size(); ++i) {
		item = (String)_cqSchema.get(i);

		if(isStaticString(item)) {
		    cqSB.append(item.substring(1, item.length()-1));
		} else {
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
	    StringBuilder valSB = new StringBuilder(_valSchema.size());

	    for(int i = 0; i < _valSchema.size(); ++i) {
		item = (String)_valSchema.get(i);

		if(isStaticString(item)) {
		    valSB.append(item.substring(1, item.length()-1));
		} else {
		    valSB.append(t.getStringByField(item));
		}
	    }
	    _val = new Value(valSB.toString().getBytes());
	} else {
	    _val = new Value("".getBytes());
	}
    }

    public void execute(Tuple tuple) {
	setKVPair(tuple);

	Mutation m = new Mutation(_row);
	m.put(_cf, _cq, new ColumnVisibility(_cv), _ts, _val);
	if(_debug) 
	    log.info(_row.toString()+" "+_cf.toString()+":"+_cq.toString()+" ["+_cv+"] "+_val.toString());

	try {
	    _writer.addMutation(m);
	} catch(Exception e) {
	    e.printStackTrace();
	}

	_collector.ack(tuple);
    }

    public void cleanup() {
	try {
	    _writer.close();
	} catch(Exception e) {
	    e.printStackTrace();
	}
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields());
    }

    public Map getComponentConfiguration() {
	return null;
    }












}
