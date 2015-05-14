package org.brennonyork.siren.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import org.brennonyork.siren.Function;
import org.brennonyork.siren.Functor;

/**
 * An example use of a Function Bolt to manipulate elements in the stream.
 *
 * It begins by defining a Functor called <code>Double</code> which is then applied through the
 * Function Bolt onto the stream. This is the most basic of functions that can be applied given
 * that reductions, aggregations, and other manipulations can be made over mere append
 * operations.</br></br>
 * 
 * <b>Classes</b></br>
 * <table>
 *   <tr><td>Functor</td>
 *       <td>An interface to define a single function <code>eval</code> which manipulates tuple
 *           elements within the stream and returns a new <code>Values</code> object.</td></tr>
 *   <tr><td>Function</td>
 *       <td>The actual Bolt that manipulates the Storm stream. It takes the static
 *           <code>Functor</code> and calls the user-defined <code>eval</code> on each tuple
 *           emitted through it.</td></tr>
 * </table>
 * 
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York
 */
public class FunctionTopology {
    public static class Double implements Functor {
	public Values eval(Tuple tuple) {
	    Values v = new Values();
	    v.add(tuple.getString(0));
	    v.add(tuple.getString(0));
	    return v;
	}
    }

    void run(String[] args) {
	TopologyBuilder builder = new TopologyBuilder();

	builder.setSpout("word_spout", new TestWordSpout(), 2);
	builder.setBolt("tuple_double", new Function(new Double(), new Fields("word1", "word2")), 2).shuffleGrouping("word_spout");
		
	Map conf = new HashMap();
	conf.put(Config.TOPOLOGY_WORKERS, 4);
	conf.put(Config.TOPOLOGY_DEBUG, true);
		
	if(args.length==0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("MockIngest", conf, builder.createTopology());
        } else {
	    try {
		StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    } catch(Exception e) {
		e.printStackTrace();
	    }
	}
    }

    void FunctionTopology() { }

    public static void main(String[] args) {
	FunctionTopology ft = new FunctionTopology();
	ft.run(args);
    }
}
