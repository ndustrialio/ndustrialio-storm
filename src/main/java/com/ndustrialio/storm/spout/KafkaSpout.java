package com.ndustrialio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jmhunt on 3/17/17.
 */
public class KafkaSpout extends BaseNdustrialioSpout
{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);


    public String COMPONENT_ID = KafkaSpout.class.getSimpleName();

    // Timeout of a poll() call
    public static final long POLL_TIMEOUT = 100;

    // Adapts Kafka output to the topology
    protected KafkaSpoutAdapter<String, String> _spoutAdapter;

    // Consumer instance
    protected KafkaConsumer<String, String> _consumer;

    // Heron outputCollector
    protected SpoutOutputCollector _collector;

    // Topic, group ids
    // TODO: multiple topics?
    protected String _topic, _groupID;

    protected Map<String, Object> _overrideProps;



    public KafkaSpout(String topic, String groupID, KafkaSpoutAdapter<String, String> adapter)
    {
        _topic = topic;
        _groupID = groupID;
        _spoutAdapter = adapter;

        _overrideProps = new HashMap<>();
    }

    public KafkaSpout setComponentID(String id)
    {
        COMPONENT_ID = id;

        return this;
    }

    public KafkaSpout withProp(String key, Object value)
    {
        _overrideProps.put(key, value);

        return this;
    }

    @Override
    public void nextTuple()
    {

        ConsumerRecords<String, String> records = _consumer.poll(100);

        for (ConsumerRecord<String, String> record : records)
        {
            // Get message ID object and KafkaValues object
            KafkaValues values = _spoutAdapter.getValues(record);
            KafkaMessageID messageID = new KafkaMessageID(record);

            // Emit on the appropriate stream with the appropriate values
            _collector.emit(values.getStreamID(), values, messageID);
        }

    }

//    @Override
//    public void ack(Object messageID)
//    {
//        KafkaMessageID id = (KafkaMessageID)messageID;
//
//        Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
//
//        m.put(new TopicPartition(id.topic, id.partition), new OffsetAndMetadata(id.offset));
//
//        _consumer.commitSync(m);
//    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.setConf(conf);
        // Basic Kafka consumer properties
        // TODO: tune these
        Properties props = new Properties();
        props.put("bootstrap.servers", getConfigurationValue("kafka-host"));
        props.put("group.id", _groupID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.putAll(_overrideProps);

        // Create Consumer and subscribe to topic
        _consumer = new KafkaConsumer<>(props);
        _consumer.subscribe(Arrays.asList(_topic));

        LOG.info("KafkaSpout subscribed to topic: " + _topic + " with group ID: " + _groupID);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // Declare all streams in the adapter
        for(String stream : _spoutAdapter.streams())
        {
            outputFieldsDeclarer.declareStream(stream, _spoutAdapter.getFieldsForStream(stream));
        }
    }
}
