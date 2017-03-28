package com.ndustrialio.storm.test.core;

import backtype.storm.Testing;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jmhunt on 12/3/16.
 */
public class TestSpoutOutputCollector extends SpoutOutputCollector
{
    private final static Logger LOG = LoggerFactory.getLogger(TestSpoutOutputCollector.class);

    // Map is streamid-> list of queues.  One per receiving component.  The queue object
    // itself is the incoming queue of the later stage component.
    protected Map<String, List<BlockingQueue<Tuple>>> _outgoingStreams;

    protected Map<String, MkTupleParam> _outgoingTupleParams;

    public TestSpoutOutputCollector()
    {


        super(new ISpoutOutputCollector()
        {
            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId)
            {
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId)
            {

            }

            @Override
            public void reportError(Throwable error)
            {

            }
        });

        _outgoingStreams = new HashMap<>();

        _outgoingTupleParams = new HashMap<>();
    }

    public void addOutgoingStream(String streamID, MkTupleParam params, BlockingQueue<Tuple> queue)
    {
        _outgoingTupleParams.put(streamID, params);

        List<BlockingQueue<Tuple>> queuesForStream = _outgoingStreams.get(streamID);

        if (queuesForStream == null)
        {
            queuesForStream = new ArrayList<>();
            _outgoingStreams.put(streamID, queuesForStream);
        }

        queuesForStream.add(queue);

    }

    public void sendShutdown(Tuple shutdownTuple)
    {
        for(Map.Entry<String, List<BlockingQueue<Tuple>>> entry : _outgoingStreams.entrySet())
        {
            // Publish shutdown to all downstream components
            for(BlockingQueue<Tuple> outgoingStream : entry.getValue())
            {
                outgoingStream.add(shutdownTuple);
            }
        }
    }


    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId)
    {
        // Put tuple in appropriate outgoing streams
        List<BlockingQueue<Tuple>> streams = _outgoingStreams.get(streamId);

        for(BlockingQueue<Tuple> stream : streams)
        {
            stream.add(Testing.testTuple(tuple, _outgoingTupleParams.get(streamId)));
        }

        return null;
    }

    @Override
    public List<Integer> emit(List<Object> tuple)
    {
        emit(Utils.DEFAULT_STREAM_ID, tuple, null);

        return null;
    }


        @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId)
    {

    }
}
