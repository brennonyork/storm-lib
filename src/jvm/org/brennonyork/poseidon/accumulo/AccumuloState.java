package org.brennonyork.poseidon.accumulo;

import backtype.storm.tuple.Values;

import org.brennonyork.siren.KVSchema;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.ByteArrayOutputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Generates a state object for the Accumulo database.
 *
 * This object will handle all persistence state operations within a Trident Topology for an
 * Accumulo cluster. It handles writes into the cluster in a parallel fashion leveraging the
 * bulk mutations writer. This batches each set of tuples into their resulting Accumulo mutations
 * and then sends the batch into the cluster. After it flushes its buffers to maintain performance.
 * It can also handle distributed queries through a DRPC client releasing records from Accumulo
 * into the stream.</br></br>
 */
public class AccumuloState implements State {
    private static Logger log = Logger.getLogger(AccumuloState.class); 
    
    private String _zkInstName;
    private String _zkServers;
    private String _accumuloUser;
    private String _accumuloPasswd;

    private String _tableName;
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

    private List<Mutation> mutations;
    private Connector _conn;
    private BatchWriter _writer;
    private long _maxMemory = 100000;
    private long _maxLatency = 100000;
    private int _maxWriteThreads = 20;
    private BatchScanner _scanner;
    private Authorizations _authorizations;
    private int _numQueryThreads;
    
    public AccumuloState(Map stormConf, AccumuloConfig accumuloConf, Map schema) {
	try {
	    _zkInstName = accumuloConf.get(AccumuloConfig.ZOOKEEPER_INSTANCE_NAME).toString();
	    _zkServers = accumuloConf.get(AccumuloConfig.ZOOKEEPER_SERVER_LIST).toString();
	    _accumuloUser = accumuloConf.get(AccumuloConfig.ACCUMULO_USER).toString();
	    _accumuloPasswd = accumuloConf.get(AccumuloConfig.ACCUMULO_PASSWORD).toString();
	} catch(NullPointerException e) {
	    // TODO: Add error message or throw some new exception (if possible) relating to
	    //       any of these values not being set from within the AccumuloConfig
	}
	/*
	_tableName = (String)schema.get("table.name");
        _rowSchema = (List)schema.get("row");
        _cfSchema = (List)schema.get("column.family");
        _cqSchema = (List)schema.get("column.qualifier");
	_cvSchema = (String)schema.get("column.visibility");
        _tsSchema = (String)schema.get("timestamp");
        _valSchema = (List)schema.get("value");
	*/
	_tableName = (String)schema.get(KVSchema.KV_TABLE_NAME);
        _rowSchema = (List)schema.get(KVSchema.KV_ROW);
        _cfSchema = (List)schema.get(KVSchema.KV_COLUMN_FAMILY);
        _cqSchema = (List)schema.get(KVSchema.KV_COLUMN_QUALIFIER);
	_cvSchema = (String)schema.get(KVSchema.KV_COLUMN_VISIBILITY);
        _tsSchema = (String)schema.get(KVSchema.KV_TIMESTAMP);
        _valSchema = (List)schema.get(KVSchema.KV_VALUE);
		
	// Create a connection to Zookeeper
	try {
	    _conn = new ZooKeeperInstance(_zkInstName, _zkServers)
		.getConnector(_accumuloUser, _accumuloPasswd);
	} catch(Exception e) {
	    e.printStackTrace();
	}

	// Create a batch writer for Accumulo
	try {
	    _writer = _conn.createBatchWriter(_tableName, 
					      _maxMemory, 
					      _maxLatency, 
					      _maxWriteThreads);
	} catch(Exception e) {
	    e.printStackTrace();
	}
	
	// Create a batch scanner for Accumulo
	/*
	try {
	    _scanner = _conn.createBatchScanner(_tableName, 
						_authorizations, 
						_numQueryThreads);
	} catch(Exception e) {
	    e.printStackTrace();
	}
	*/
    }

    private static boolean isStaticString(String s) {
        if(s.startsWith("\"") && s.endsWith("\"")) {
            return true;
	} else {
            return false;
	}
    }

    /**
     * Used to build a correct mutation from a Tuple t whose values are primitive
     * byte arrays.
     *
     * @param t The individual tuple passed in
     */
    private void setBinaryKVPair(TridentTuple t) {
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    }

    /**
     * Used to build a correct mutation from a Tuple t whose values are strings.
     *
     * @param t The individual tuple passed in
     */
    private void setKVPair(TridentTuple t) {
        StringBuilder rowSB = new StringBuilder(_rowSchema.size());
        String fieldLabel;

        /// ROW ID
        for(int i = 0; i < _rowSchema.size(); ++i) {
            fieldLabel = (String)_rowSchema.get(i);

            if(isStaticString(fieldLabel)) {
                rowSB.append(fieldLabel.substring(1, fieldLabel.length()-1));
            } else {
                rowSB.append(t.getStringByField(fieldLabel));
            }
        }
        _row = new Text(rowSB.toString());

        /// COLUMN FAMILY
        if(!(_cfSchema == null) && !(_cfSchema.isEmpty())) {
            StringBuilder cfSB = new StringBuilder(_cfSchema.size());

            for(int i = 0; i < _cfSchema.size(); ++i) {
		fieldLabel = (String)_cfSchema.get(i);

                if(isStaticString(fieldLabel)) {
		    cfSB.append(fieldLabel.substring(1, fieldLabel.length()-1));
                } else {
                    cfSB.append(t.getStringByField(fieldLabel));
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
                fieldLabel = (String)_cqSchema.get(i);

                if(isStaticString(fieldLabel)) {
                    cqSB.append(fieldLabel.substring(1, fieldLabel.length()-1));
                } else {
                    cqSB.append(t.getStringByField(fieldLabel));
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
                fieldLabel = (String)_valSchema.get(i);

                if(isStaticString(fieldLabel)) {
                    valSB.append(fieldLabel.substring(1, fieldLabel.length()-1));
                } else {
                    valSB.append(t.getStringByField(fieldLabel));
		}
            }
	    _val = new Value(valSB.toString().getBytes());
        } else {
            _val = new Value("".getBytes());
	}
    }

    public void beginCommit(Long txid) {
	log.debug("beginCommit["+txid+"]");
    }

    public void commit(Long txid) {
	log.debug("commit["+txid+"]");
    }
	
    public void bulkSet(List<TridentTuple> tuples) {	
	mutations = new ArrayList<Mutation>();

        for(TridentTuple tuple : tuples) {
            setKVPair(tuple);
	    Mutation m = new Mutation(_row);
            m.put(_cf, _cq, new ColumnVisibility(_cv), _ts, _val);
            mutations.add(m);
        }
	try {
	    _writer.addMutations(mutations);
	    _writer.flush();	    
	} catch(MutationsRejectedException e) {
	    // TODO: This
	}
    }
	
    public List<String> bulkGet(List<String> queries) {
	return new ArrayList<String>();
    }
}
