package com.ndustrialio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import org.json.JSONObject;

import java.util.Map;

/**
 * Created by jmhunt on 6/24/16.
 */
public interface ISpoutLifecycle
{

    // Called when a tuple is acked or failed
    void onTupleAck(Object id);
    void onTupleFail(Object id);

    void onPreOpen(Map conf, TopologyContext context,
                   SpoutOutputCollector collector);

    void onPostOpen(Map conf, TopologyContext context,
                    SpoutOutputCollector collector);


}
