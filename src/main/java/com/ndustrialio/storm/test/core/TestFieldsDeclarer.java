package com.ndustrialio.storm.test.core;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmhunt on 12/1/16.
 */
public class TestFieldsDeclarer implements OutputFieldsDeclarer
{

    protected Map<String, Fields> _declaredStreams;

    public TestFieldsDeclarer()
    {
        _declaredStreams = new HashMap<>();
    }
    @Override
    public void declare(Fields fields)
    {
        _declaredStreams.put(Utils.DEFAULT_STREAM_ID, fields);
    }

    @Override
    public void declare(boolean direct, Fields fields)
    {

    }

    public Map<String, Fields> getDeclaredStreams()
    {
        return _declaredStreams;
    }

    @Override
    public void declareStream(String streamId, Fields fields)
    {
        _declaredStreams.put(streamId, fields);
    }

    @Override
    public void declareStream(String streamId, boolean direct, Fields fields)
    {

    }
}
