package org.brennonyork.poseidon.accumulo;

import backtype.storm.tuple.Values;

import org.brennonyork.poseidon.accumulo.AccumuloState;

import org.apache.log4j.Logger;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class AccumuloUpdater extends BaseStateUpdater<AccumuloState> {
    private static Logger log = Logger.getLogger(AccumuloUpdater.class);

    public AccumuloUpdater() {

    }

    public void updateState(AccumuloState state, 
			    List<TridentTuple> tuples, 
			    TridentCollector collector) {
	// Set tuples into mutations and insert them into Accumulo
	state.bulkSet(tuples);
    }
}
