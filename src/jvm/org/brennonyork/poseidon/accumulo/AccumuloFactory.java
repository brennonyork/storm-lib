package org.brennonyork.poseidon.accumulo;

//import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import org.brennonyork.poseidon.accumulo.AccumuloState;
import org.brennonyork.siren.KVSchema;

import org.apache.log4j.Logger;

import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class AccumuloFactory implements StateFactory {
    private static Logger log = Logger.getLogger(AccumuloFactory.class);

    private AccumuloConfig _config;
    private Map _schema;

    /**
     * @param config AccumuoConfig object with each variable set for the given cluster
     * @param schema KVSchema representing how this data will be placed into Accumulo
     */
    public AccumuloFactory(AccumuloConfig config, KVSchema schema) {
	_config = config;
	_schema = schema.serialize();
    }
	
    /**
     * @return a new AccumuloState instance.
     *
    public State makeState(Map conf, 
			   IMetricsContext metrics, 
			   int partitionIndex, 
			   int numPartitions) {
	return new AccumuloState(conf, _config, _schema);
    }*/

    public State makeState(Map conf, 
			   int partitionIndex, 
			   int numPartitions) {
	return new AccumuloState(conf, _config, _schema);
    }
}
