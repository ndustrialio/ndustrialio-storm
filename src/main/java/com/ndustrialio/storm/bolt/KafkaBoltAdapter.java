package com.ndustrialio.storm.bolt;


import backtype.storm.tuple.Tuple;

import java.io.Serializable;

/**
 * Created by jmhunt on 3/20/17.
 */
public interface KafkaBoltAdapter extends Serializable
{
    /**
     * Get the Kafka topic this tuple should be emitted on
     * @param tuple The incoming tuple
     * @return The topic
     */
    abstract String getTopicFromTuple(Tuple tuple);

    /**
     * Get key of the ProducerRecord generated from this tuple
     * @param tuple The incoming tuple
     * @return The ProducerRecord key
     */
    String getKeyFromTuple(Tuple tuple);

    /**
     * Get value of the ProducerRecord generated from this tuple
     * @param tuple The incoming tuple
     * @return The ProducerRecord value
     */
    abstract String getValueFromTuple(Tuple tuple);
}
