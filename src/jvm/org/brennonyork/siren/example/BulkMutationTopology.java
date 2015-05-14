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
import java.util.UUID;

import org.brennonyork.siren.KVSchema;
import org.brennonyork.siren.BulkMutation;

/**
 * Demonstrates how to parse a binary SiLK file into a Storm stream.
 *
 * This simple class leverages three core pieces of the Siren library to inject SiLK records
 * into the Storm stream. Files are found in a given directory, parsed into their constituent
 * pieces, and finally emitted into the stream.</br></br>
 * 
 * <b>Classes</b></br>
 * <table>
 *   <tr><td>RecordTemplate</td>
 *       <td>Used as an API to objectify a record into a language Java understands. This
 *           format is read by the <code>Parse</code> class to correctly determine how
 *           to read the individual files.</td></tr>
 *   <tr><td>PollDirectory</td>
 *       <td>Polls the given directory for SiLK binary records and moves them into their
 *           corresponding processing directory. The absolute path of the processing directory
 *           is emitted into the stream.</td></tr>
 *   <tr><td>Parse</td>
 *       <td>Leveraging the <code>Record Template</code> it reads files off the stream and
 *           parses them into records. Those records are then output to the stream.</td></tr>
 * </table>
 * 
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York
 */
public class BulkMutationTopology {
    void run(String[] args) {
	TopologyBuilder builder = new TopologyBuilder();

	KVSchema names = new KVSchema("names", "word");
	names.colFam.addStatic("NAME");
	names.colQual.add("word").addStatic("\u0000").add("word");

	builder.setSpout("word_spout", new TestWordSpout(), 2);
	builder.setBolt("mutate", new BulkMutation(names), 2).shuffleGrouping("word_spout");
		
	Map conf = new HashMap();
	conf.put(Config.TOPOLOGY_WORKERS, 4);
	conf.put(Config.TOPOLOGY_DEBUG, true);
	conf.put("bulk.output.path", "/tmp/rf");
	conf.put("bulk.write.timeout", 10);
	conf.put("bulk.max.records", 1000);
		
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

    void BulkMutationTopology() { }

    public static void main(String[] args) {
	BulkMutationTopology bmt = new BulkMutationTopology();
	bmt.run(args);
    }
}
