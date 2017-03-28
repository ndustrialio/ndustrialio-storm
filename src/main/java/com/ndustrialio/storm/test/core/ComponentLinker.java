package com.ndustrialio.storm.test.core;

import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jmhunt on 12/4/16.
 */
public class ComponentLinker
{

    private final static Logger LOG = LoggerFactory.getLogger(ComponentLinker.class);

    private Map<String, List<String>> _streams;

    private TestTopologyBuilder _builder;
    private ComponentTestHarness _harness;


    public ComponentLinker(ComponentTestHarness harness, TestTopologyBuilder builder)
    {
        _streams = new HashMap<>();

        _harness = harness;
        _builder = builder;
    }

    public ComponentLinker shuffleGrouping(String componentID)
    {
        return shuffleGrouping(componentID, Utils.DEFAULT_STREAM_ID);
    }


    public ComponentLinker shuffleGrouping(String componentID, String streamID)
    {
        List<String> streamsForComponent = _streams.get(componentID);

        if (streamsForComponent == null)
        {
            streamsForComponent = new ArrayList<>();
            _streams.put(componentID, streamsForComponent);
        }

        streamsForComponent.add(streamID);


        return this;
    }

    public void link()
    {
        // Iterate over all components that want to receive from our harness
        for(Map.Entry<String, List<String>> entry : _streams.entrySet())
        {
            // Get downsteam harness (a tuple sink)
            ComponentTestHarness sink = _builder.getTestHarness(entry.getKey());

            if (sink == null)
            {
                throw new IllegalArgumentException("Component "
                        + entry.getKey() + " subscribes to component " + _harness.getComponentID()
                        + " but has not been declared!");
            }

            // Get the downstream harness' incoming queue
            // We will use it as our outgoing queue for this stream.
            // i.e. this harness publishes to later stages incoming queues.
            BlockingQueue<Tuple> inboundQueue = sink.getIncomingStream();

            // Connect all streams
            for (String streamID : entry.getValue())
            {
                LOG.info("Linking stream " + streamID + " from source component " + _harness.getComponentID()
                        + " to sink component " + sink.getComponentID());

                _harness.setOutgoingStream(streamID, inboundQueue);
            }

        }
    }
}
