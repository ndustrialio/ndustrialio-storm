package com.ndustrialio.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jmhunt on 3/20/17.
 */
public class KafkaBolt extends BaseNdustrialioBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBolt.class);

    protected KafkaBoltAdapter _adapter;

    protected KafkaProducer<String, String> _producer;

    protected OutputCollector _collector;

    protected Map<String, Object> _overrideProps;

    public KafkaBolt(KafkaBoltAdapter adapter)
    {
        _adapter = adapter;

        _overrideProps = new HashMap<>();
    }

    public KafkaBolt withProp(String key, Object value)
    {
        _overrideProps.put(key, value);

        return this;
    }



    @Override
    public void execute(Tuple tuple)
    {
        try
        {
            ProducerRecord<String, String> newRecord =
                    new ProducerRecord<>(_adapter.getTopicFromTuple(tuple),
                            _adapter.getKeyFromTuple(tuple),
                            _adapter.getValueFromTuple(tuple));

            _producer.send(newRecord);

            _collector.ack(tuple);

        } catch (Exception e)
        {
            // TODO: determine when we can ack and when we can fail
            LOG.error(e.getMessage(), e);
            _collector.ack(tuple);
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        this.setConf(conf);
        _collector = collector;

        // TODO: tune defaults
        Properties props = new Properties();
        props.put("bootstrap.servers", getConfigurationValue("kafka-host"));
        props.put("linger.ms", 1L);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Apply override props
        props.putAll(_overrideProps);

        _producer = new KafkaProducer<>(props);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {

    }
}
