package com.ndustrialio.storm.bolt;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import com.ndustrialio.core.BaseConfiguredComponent;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public abstract class BaseNdustrialioBolt extends BaseConfiguredComponent implements IRichBolt
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1367851046906622603L;
	
    public void cleanup() {
    }    
    
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public abstract void execute(Tuple tuple);

	public String stackTraceToString(Throwable t)
	{
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);

        return sw.toString();
	}

}
