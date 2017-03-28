package com.ndustrialio.storm.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by jmhunt on 3/17/17.
 */
public class KafkaMessageID
{
    public String topic;
    public long offset;
    public int partition;

    public KafkaMessageID(ConsumerRecord record)
    {
        this.topic = record.topic();
        this.offset = record.offset();
        this.partition = record.partition();
    }
}
