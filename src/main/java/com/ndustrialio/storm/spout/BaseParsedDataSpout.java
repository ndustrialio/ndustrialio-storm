package com.ndustrialio.storm.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.ndustrialio.messaging.RabbitQueueOptions;
import org.json.JSONObject;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class BaseParsedDataSpout extends BaseRabbitSpout
{

	

	public BaseParsedDataSpout(String queueName, String routingKey, Class loggerClass)
	{

		super(new RabbitQueueOptions(queueName), "parsed-exchange", routingKey, loggerClass);
		
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

	@Override
	public Object onNextTuple(JSONObject obj, Object msg_id, String routingKey)
	{
			int window = obj.getInt("window");
			int outputID = obj.getInt("output_id");
			int fieldID = obj.getInt("field_id");
			String timestamp = obj.getString("timestamp");
			String humanName = obj.getString("field_human_name");
			String value = obj.getString("value");
			String value_type = obj.getString("value_type");
			boolean totalizer = routingKey.contains("totalizer");
			
			_collector.emit(new Values(window, 
					outputID, fieldID, timestamp, humanName, value, value_type, totalizer), msg_id);
			
			return msg_id;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("window", "output_id", "field_id", "timestamp", "field_human_name", "value", "value_type", "is_totalizer"));

	}


}
