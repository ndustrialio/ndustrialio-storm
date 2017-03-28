package com.ndustrialio.storm.test.core;

import backtype.storm.tuple.Tuple;

import java.util.concurrent.BlockingQueue;

/**
 * Created by jmhunt on 12/2/16.
 */
public abstract class ComponentTestHarness implements Runnable
{
    protected TestFieldsDeclarer _declarer;

    protected String _componentID;

    public ComponentTestHarness(String componentID)
    {
        _componentID = componentID;
        _declarer = new TestFieldsDeclarer();

    }

    public abstract BlockingQueue<Tuple> getIncomingStream();
    public abstract void setOutgoingStream(String streamID, BlockingQueue<Tuple> stream);

    public String getComponentID()
    {
        return _componentID;
    }

    public ComponentLinker getComponentLinker(TestTopologyBuilder builder)
    {
        return new ComponentLinker(this, builder);
    }

    public abstract void run();
}
