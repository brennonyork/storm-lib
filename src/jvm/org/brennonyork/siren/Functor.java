package org.brennonyork.siren;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.Serializable;

/**
 * Interface for developers to define functions for execution on Tuples. 
 * Used in tandem with the Function class.
 *
 * @author Brennon York
 */
public interface Functor extends Serializable {
    /**
     * This will be the function that is applied to each tuple in the stream
     * that passes through this bolt.
     *
     * @param tuple the input Storm Tuple that is provided through the
     *              Function class
     * @return new Values object that contains all fields declared in the
     *             constructing Function class where this is called
     */
    public Values eval(Tuple tuple);
}
