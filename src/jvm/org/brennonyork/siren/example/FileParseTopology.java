package org.brennonyork.siren.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

import org.brennonyork.siren.RecordTemplate;
import org.brennonyork.siren.PollDirectory;
import org.brennonyork.siren.Parse;

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
public class FileParseTopology {

    void run(String[] args) {
	TopologyBuilder builder = new TopologyBuilder();

	// Build the record template for a given SiLK record
	RecordTemplate SilkRecord = 
	    (new RecordTemplate())
	    .readBytes(8, "ts")
	    .readBytes(4, "dur")
	    .readBytes(2, "sport")
	    .readBytes(2, "dport")
	    .readBytes(1, "proto")
	    .readBytes(1, "ct")
	    .readBytes(2, "sensor")
	    .readBytes(1, "flags")
	    .readBytes(1, "init_flags")
	    .readBytes(1, "sess_flags")
	    .readBytes(1, "attr")
	    .readBytes(2, "appl")
	    .skipBytes(2)
	    .readBytes(2, "snmp_in")
	    .readBytes(2, "snmp_out")
	    .readBytes(4, "packets")
	    .readBytes(4, "bytes")
	    .readBytes(4, "sip")
	    .readBytes(4, "dip")
	    .readBytes(4, "nhip");

	builder.setSpout("poll_spout", new PollDirectory("/tmp/silk/dir", "/tmp/silk/dir/proc"), 1);
	builder.setBolt("silk_atomize", new Parse(SilkRecord), 2).shuffleGrouping("poll_spout");
		
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

    void FileParseTopology() { }

    public static void main(String[] args) {
	FileParseTopology fpt = new FileParseTopology();
	fpt.run(args);
    }
}
