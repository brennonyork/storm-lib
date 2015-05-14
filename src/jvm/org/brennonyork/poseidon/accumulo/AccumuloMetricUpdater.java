package org.brennonyork.poseidon.accumulo;

import backtype.storm.tuple.Values;

import org.brennonyork.poseidon.accumulo.AccumuloState;

import org.apache.log4j.Logger;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class AccumuloMetricUpdater extends BaseStateUpdater<AccumuloState> {
    private static Logger log = Logger.getLogger(AccumuloMetricUpdater.class);

    private long batchStart;
    private long _clusterWriteTime = 0;
    private long _numRecords = 0;
    private long _numBatchesPerStat = 1000;
    private long _currBatch = 0;
    private long _startTime = 0;

    public AccumuloMetricUpdater() {
	this(1000);
    }

    public AccumuloMetricUpdater(long numBatchesPerStat) {
	_numBatchesPerStat = numBatchesPerStat;
	_startTime = System.currentTimeMillis();
    }

    public void updateState(AccumuloState state, 
			    List<TridentTuple> tuples, 
			    TridentCollector collector) {
	// Preconditions for statistics collection
	batchStart = System.currentTimeMillis();
	// Set tuples into mutations and insert them into Accumulo
	state.bulkSet(tuples);
	// Postconditions for statistics collection
	_clusterWriteTime += (System.currentTimeMillis() - batchStart);
	_numRecords += tuples.size();
	// Check if the number of batches has exceeded the number for statistics calculations
	if(++_currBatch % _numBatchesPerStat == 0) {
	    log.info("AvgWrite:   "+((float)_clusterWriteTime / (float)_numBatchesPerStat)+"ms");
	    log.info("AvgLatency: "+(System.currentTimeMillis() - _startTime)+"ms");
	    log.info("AvgNumRecs: "+(_numRecords / _numBatchesPerStat)+" per batch");
	    _clusterWriteTime = 0;
	    _numRecords = 0;
	    _currBatch = 0;
	    _startTime = System.currentTimeMillis();
	    collector.emit(new Values(((float)_clusterWriteTime / (float)_numBatchesPerStat),
				      (System.currentTimeMillis() - _startTime),
				      ((float)_numRecords / (float)_numBatchesPerStat)));
	}
    }
}
