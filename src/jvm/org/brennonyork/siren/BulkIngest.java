package org.brennonyork.siren;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;

/**
 * Handles RFile ingestion on a table within an Accumulo instance from emitted Storm Tuples.
 *
 * Each process creates its own registry to maintain RFiles and their corresponding tables
 * such that the process can scale linearly across machines. It also scales well because it
 * can be setup as a shuffle grouping from the BulkMutation Bolt to linearly disperse RFiles
 * across a cluster.</br></br>
 *
 * This process creates a directory structure under HDFS which looks as follows:</br>
 * <code>/<i>hdfsRoot</i>/<i>localRoot</i>/<i>componentId</i>/<i>tableName</i>/{success|failure}/</code></br></br>
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
public class BulkIngest extends BaseRichBolt {
    static Logger log = Logger.getLogger(BulkIngest.class);
	
    private OutputCollector _collector;
    private Connector _conn;
    private FileSystem _fs;
    private TableOperations _tableOps;
    private boolean _debug = false;

    private String _localRoot;
    private String _tableRoot;
    private String _tableName;
    private String _failureDir;
    private Path _rfilePath;
    private Path _rfileIngestPath;
    private Path _successPath;
    private Path _failurePath;
    private Map<String, String> _registry;

    private String _hdfsRoot = "/tmp/rfile";
    private String _zkInstName = "myinstance";
    private String _zkServers = "localhost";
    private String _AccumuloUser = "root";
    private String _AccumuloPasswd = "passwd";
    private int _maxTimeInterval = 10; // 5 minutes (in seconds)

    /**
     * @param hdfsRoot The local root within HDFS for all Bulk Ingest processing and file
     *                 manipulation.
     * @author Brennon York
     */
    public BulkIngest(String hdfsRoot) {
	this(hdfsRoot, false);
    }

    /**
     * @param hdfsRoot The local root within HDFS for all Bulk Ingest processing and file
     *                 manipulation.
     * @param debug Predicate to determine if this bolt should instantiate a MockInstance class
     *              for connecting with ZooKeeper or not.
     * @author Brennon York
     */
    public BulkIngest(String hdfsRoot, boolean debug) {
	_hdfsRoot = hdfsRoot;
	_debug = debug;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	_collector = collector;
	_registry = new HashMap<String,String>();

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
		
	try {
	    _fs = FileSystem.get(new Configuration());
	} catch(IOException e) {
	    e.printStackTrace();
	}

	// Should ensure a unique 'root' within HDFS for the given process
	_localRoot = _hdfsRoot.concat(Path.SEPARATOR).concat(Integer.toString(context.getThisTaskId()));
	
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

	_tableOps = _conn.tableOperations();
    }

    public void execute(Tuple tuple) {
	_tableName = tuple.getStringByField("table-name");
	_rfilePath = new Path(tuple.getStringByField("abs-rfile-path"));

	// If we already have a file from this table name then move it into
	// that respective directory and add it to the list
	// 1. _registry.get(tableName) should return the directory where all rfiles
	// reside for the given table
	// 2. Because this is distributed _registry.get() should be a unique folder
	// structure to each bolt
	if(!_registry.containsKey(_tableName)) {
	    _registry.put(_tableName, _localRoot.concat(Path.SEPARATOR).concat(_tableName));
	}
	// Otherwise if we haven't seen this table before then create it in the
	// local registry, move the respective rfile into the correct directory
	// structure in HDFS, and finally add it to the registry list
	
	// Set any incoming rfiles into the specified directory
	
	_tableRoot = _registry.get(_tableName).toString();
	_rfileIngestPath = new Path(_tableRoot
				    .concat(Path.SEPARATOR)
				    .concat("success")
				    .concat(Path.SEPARATOR)
				    .concat(_rfilePath.getName()));
	_successPath = new Path(_tableRoot
				.concat(Path.SEPARATOR)
				.concat("success"));
	
	try {
	    if(!_fs.exists(_successPath)) {
		try {
                    _fs.mkdirs(_successPath);
		} catch(IOException e) {
		    e.printStackTrace();
		}
	    }
	
	    _fs.rename(_rfilePath, _rfileIngestPath);
	} catch (IOException e) {
	    e.printStackTrace();
	}

	// If we're past our time limit then bulk ingest all data local to our
	// registry instance
	if(((int)(System.currentTimeMillis() / 1000 /* Convert to seconds */) % _maxTimeInterval) == 0) {	    
	    for (Map.Entry<String, String> entry : _registry.entrySet()) {
		_tableName = entry.getKey();
		_tableRoot = entry.getValue();

		if(!_conn.tableOperations().exists(_tableName)) {
		    try {
			_conn.tableOperations().create(_tableName);
		    } catch(Exception e) {
			e.printStackTrace();
		    }
		}

		if(_debug) {
		    _failureDir = "";
		    _failurePath = new Path(_failureDir);
		} else {
		    _failureDir = _tableRoot
			.concat(Path.SEPARATOR)
			.concat("failure");
		    _failurePath = new Path(_failureDir);
		}

		try {
		    if(!_fs.exists(_failurePath)) {
			try {
			    _fs.mkdirs(_failurePath);
			} catch(IOException e) {
			    e.printStackTrace();
			}
		    }

		    _conn.tableOperations().importDirectory(_tableName, 
							    _tableRoot.concat(Path.SEPARATOR).concat("success"),
							    _failureDir,
							    true);
		} catch (TableNotFoundException e) {
		    e.printStackTrace();
		} catch (IOException e) {
		    e.printStackTrace();
		} catch (AccumuloException e) {
		    e.printStackTrace();
		} catch (AccumuloSecurityException e) {
		    e.printStackTrace();
		}
	    }
	}

	_collector.ack(tuple);
    }

    public void cleanup() {
	// TODO: serialize the local registry to disk!!
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields());
    }

    public Map getComponentConfiguration() {
	return null;
    }
}
