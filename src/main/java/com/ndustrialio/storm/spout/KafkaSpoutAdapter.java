package com.ndustrialio.storm.spout;

import backtype.storm.tuple.Fields;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Created by jmhunt on 3/17/17.
 */
public interface KafkaSpoutAdapter<K, V> extends Serializable
{
    /**
     * Transform a Kafka ConsumerRecord into a KafkaValues object
     * @param record The ConsumerRecord from Kafka
     * @return a KafkaValues instance, optionally with its stream set
     */
     KafkaValues getValues(ConsumerRecord<K, V> record);

    /**
     * Get the Fields object for a stream
     * @param streamID the stream to get fields for
     * @return fields object for this stream
     */
     Fields getFieldsForStream(String streamID);

    /**
     * Get the streams that this Spout should declare
     * @return List of streams
     */
     List<String> streams();
}
