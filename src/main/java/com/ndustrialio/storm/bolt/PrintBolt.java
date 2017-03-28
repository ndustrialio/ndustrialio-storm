package com.ndustrialio.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by jmhunt on 12/7/16.
 */
public class PrintBolt extends BaseNdustrialioBolt
{
    public static final String COMPONENT_ID = PrintBolt.class.getSimpleName();

    private static final Logger LOG = LoggerFactory.getLogger(PrintBolt.class);


    private OutputCollector _collector;


    public void print(Tuple tuple)
    {
        StringBuilder sb = new StringBuilder("Received tuple from : " + tuple.getSourceComponent() + "\n"
                            + " on stream: " + tuple.getSourceStreamId() + "\n");


        for(String field : tuple.getFields())
        {
            sb.append(field);
            sb.append(": ");
            sb.append(tuple.getValueByField(field).toString());
            sb.append(" ");
        }

        LOG.info(sb.toString());

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.setConf(stormConf);

        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        print(tuple);

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // No fields from here
    }

}
