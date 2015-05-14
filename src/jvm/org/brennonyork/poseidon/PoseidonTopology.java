package org.brennonyork.poseidon;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.brennonyork.siren.KVSchema;
import org.brennonyork.poseidon.accumulo.AccumuloConfig;
import org.brennonyork.poseidon.accumulo.AccumuloFactory;
import org.brennonyork.poseidon.accumulo.AccumuloMetricUpdater;

import org.apache.log4j.Logger;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.HashMap;

public class PoseidonTopology {
    private AccumuloConfig accumuloConfig = new AccumuloConfig();

    public static class MyFunction extends BaseFunction {
	public void execute(TridentTuple tuple, TridentCollector collector) {
	    for(int i=0; i < tuple.getFloat(0); i++) {
		collector.emit(new Values(i));
	    }
	}
    }

    void run(String[] args) {
	/*
	clusterConfig.zkInstName = "dev_instance";
	clusterConfig.zkServers = "localhost";
	clusterConfig.accumuloUser = "root";
	clusterConfig.accumuloPasswd = "dev_instance";
	*/
	
	accumuloConfig.setZookeeperInstName("dev_instance");
	accumuloConfig.setZookeeperServers("localhost");
	accumuloConfig.setAccumuloUser("root");
	accumuloConfig.setAccumuloPassword("dev_instance");
	
	KVSchema schema = new KVSchema("test", "sentence");
	
	FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 120,
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"),
						    new Values("the cow jumped over the moon"),
						    new Values("the man went tobought some candy"),
						    new Values("four score and seven years ago"),
						    new Values("how many apples can you eat"),
						    new Values("to be or not to be the person"));
        spout.setCycle(true);

	TridentTopology topology = new TridentTopology();
	//TridentState accumulo = 
	topology
	    .newStream("accumulo", spout).parallelismHint(10)
	    .partitionPersist(new AccumuloFactory(accumuloConfig, schema), 
			      schema.getFields(), 
			      new AccumuloMetricUpdater(2000), new Fields("avg-write","avg-latency","avg-records"))
	    .newValuesStream()
	    .each(new Fields("avg-write"), new MyFunction(), new Fields("new-write"));

    /*
      TridentTopology topology = new TridentTopology();
      TridentState locations = topology.newStaticState(new AccumuloDBFactory());
      topology.newStream("myspout", spout)
      .stateQuery(locations, new Fields("userid"), new QueryLocation(), new Fields("location"))
    */

	Config conf = new Config();
	//conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1);
	//conf.setDebug(true);

	if(args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("mock", conf, topology.build());
	} else {
	    try {
		StormSubmitter.submitTopology(args[0], conf, topology.build());
	    } catch(Exception e) {

	    }
	}

    }

    public static void main(String[] args) {
	PoseidonTopology pt = new PoseidonTopology();
	pt.run(args);
    }
}
