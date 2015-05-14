package org.brennonyork.siren;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.brennonyork.siren.Functor;

import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Bolt which leverages the Functor object to easily operate on tuples emitted through
 * the stream.
 *
 * This bolt provides a simplistic interface into tuple stream operations. It was made as
 * an extensible option for either easier development in relation to Storm tuples or as a
 * method of injecting functionality into the stream at runtime. In both cases the Function
 * object takes care of all necessary Storm dynamics including reliable messaging and 
 * acknowledgement. Because the Function, and consequently the Functor, operate over the
 * entire tuple one can generate reduction functions, aggregation, find-replace, and a host
 * of other possibilities over operating on a single field within a Tuple.</br></br>
 *
 * <b>OutputFieldsDeclaration</b></br>
 * <table>
 *   <tr><td><i>dynamic</i></td>
 *       <td>The output fields are declared on construction of the object and, as such, are
 *           generated dynamically by user action.</td></tr>
 * </table>
 * 
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York
 */
public class Function extends BaseRichBolt {
    /** Logger object for all logging capabilities */
    static Logger log = Logger.getLogger(Function.class);
    /** OutputCollector object registered to emit new tuples into the stream */
    private OutputCollector _collector;
    /** instance of the Functor object passed in on instantiation */
    private Functor _f;
    /** Fields declaration to pass through to the declareOutputFields method */
    private Fields _outputFields;

    /**
     * Constructor defining the Functor to evaluate per Tuple and the corresponding output Fields.
     *
     * @param functor Functor which declares the user-defined function to evaluate
     * @param outputFields Fields object for Storm to pass through to the 
     *                     declareOutputFields() method
     */
    public Function(Functor functor, Fields outputFields) { 
	_f = functor;
	_outputFields = outputFields;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	_collector = collector;
    }

    public void execute(Tuple tuple) {
	_collector.emit(tuple, _f.eval(tuple));
	_collector.ack(tuple);
    }

    public void cleanup() { }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(_outputFields);
    }

    public Map getComponentConfiguration() {
	return null;
    }
}
