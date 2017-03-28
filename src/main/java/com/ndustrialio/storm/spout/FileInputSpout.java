package com.ndustrialio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jmhunt on 12/15/16.
 */
public class FileInputSpout extends BaseNdustrialioSpout
{
    public static final String COMPONENT_ID = FileInputSpout.class.getSimpleName();
    protected SpoutOutputCollector _collector;

    protected List<String> _fileLines;

    protected List<String> _fields;

    protected int _index = 0;

    public FileInputSpout(String filename)
    {

        _fileLines = new LinkedList<>();
        try
        {
             _fileLines = Files.readAllLines(Paths.get(filename));

        } catch (IOException e)
        {
            throw new RuntimeException("Unable to read from " + filename + ": " + e.getMessage());
        }

        // Get keys to use for fields declaration
        JSONObject obj = new JSONObject(_fileLines.get(_index));

        _fields = new ArrayList<>();

        for(Object key : obj.keySet())
        {
            _fields.add(key.toString());
        }
    }

    public FileInputSpout shuffle()
    {
        Collections.shuffle(_fileLines);

        return this;
    }

    public FileInputSpout reverse()
    {
        Collections.reverse(_fileLines);

        return this;
    }

    @Override
    public void nextTuple()
    {
        String line = _fileLines.get(_index);

        JSONObject obj = new JSONObject(line);

        Values v = new Values();

        for(String field : _fields)
        {

            v.add(obj.get(field));
        }

        _collector.emit(v);

        _index++;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        _collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(_fields));
    }


}
