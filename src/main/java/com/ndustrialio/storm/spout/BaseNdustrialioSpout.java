package com.ndustrialio.storm.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;


import com.ndustrialio.core.BaseConfiguredComponent;

public abstract class BaseNdustrialioSpout extends BaseConfiguredComponent implements IRichSpout, ISpoutLifecycle
{

	
	public abstract void nextTuple();
	
	public abstract void open(Map conf, TopologyContext context, SpoutOutputCollector collector);
	
	public void close()
	{
		// TODO Auto-generated method stub
		
	}

	public void activate()
	{
		// TODO Auto-generated method stub
		
	}

	public void deactivate()
	{
		// TODO Auto-generated method stub
		
	}
	
    
    public void ack(Object msgId) {
    }

    
    public void fail(Object msgId) {
    }

	public Map<String, Object> getComponentConfiguration()
	{
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void onTupleAck(Object id)
	{

	}

	@Override
	public void onTupleFail(Object id)
	{

	}

	@Override
	public void onPreOpen(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{

	}

	@Override
	public void onPostOpen(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{

	}
}
