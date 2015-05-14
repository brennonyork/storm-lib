package org.brennonyork.poseidon.accumulo;

import backtype.storm.tuple.Values;

import org.brennonyork.poseidon.accumulo.AccumuloState;

import org.apache.accumulo.core.data.Range;

import org.apache.log4j.Logger;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class AccumuloQuery extends BaseQueryFunction<AccumuloState, String /* result type */> {
    private static Logger log = Logger.getLogger(AccumuloQuery.class);

    /**
     * This method is called as a wrapper from Trident to the AccumuloState bulkGet() method. It
     * passes a list of Trident tuples as input and expects a List of return values. These values
     * are then emitted into the stream.
     *
     * @param state The AccumuloState object which will define which database to query
     * @param inputs The list of Trident tuples to use as input to the query
     *
     * @return A list of results from the query to be passed to the execute() method for injection
     *         into the stream.
     *
     * @see AccumuloState#bulkGet(List)
     * @see #execute(TridentTuple, String, TridentCollector)
     */
    public List<String> batchRetrieve(AccumuloState state, List<TridentTuple> inputs) {
	/*
	List<Long> userIDs = new ArrayList<Long>();
	for(TridentTuple input : inputs) {
	    userIDs.add(input.getLong(0));
	}
	return state.bulkGet(userIDs);
	*/
	return new ArrayList<String>();
    }
    
    /**
     * Called for each resulting item within the List returned from the batchRetrieve() method.
     */
    public void execute(TridentTuple tuple, 
			String location /* result */, 
			TridentCollector collector) {
	collector.emit(new Values(location));
    }
}
