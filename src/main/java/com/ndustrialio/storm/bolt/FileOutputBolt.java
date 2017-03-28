package com.ndustrialio.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Map;

/**
 * Created by jmhunt on 12/15/16.
 */
public class FileOutputBolt extends BaseNdustrialioBolt
{
    public static final String COMPONENT_ID = FileOutputBolt.class.getSimpleName();
    private String _filename;

    private OutputCollector _collector;

    public FileOutputBolt(String filename)
    {
        _filename = filename;

        // Make sure the file is there
        try
        {
            Files.deleteIfExists(Paths.get(_filename));

            Files.createFile(Paths.get(_filename));

        } catch (IOException e)
        {
            throw new RuntimeException("Error creating file: " + _filename);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        try
        {
            JSONObject obj = new JSONObject();

            for(String field : tuple.getFields())
            {
                Object value = tuple.getValueByField(field);

                if (value instanceof Date)
                {
                    DateTime dt = new DateTime(value);

                    value = dt.toString("yyyy-MM-dd HH:mm:ss");
                }
                obj.put(field, value);
            }

            String objStr = obj.toString()+"\n";

            Files.write(Paths.get(_filename), objStr.getBytes(), StandardOpenOption.APPEND);

            _collector.ack(tuple);
        } catch (Exception e)
        {
            _collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }
}
