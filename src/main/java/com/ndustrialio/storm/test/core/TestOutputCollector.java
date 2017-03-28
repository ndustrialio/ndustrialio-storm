package com.ndustrialio.storm.test.core;

import backtype.storm.Testing;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jmhunt on 12/1/16.
 */
public class TestOutputCollector extends OutputCollector
{
    private final static Logger LOG = LoggerFactory.getLogger(TestOutputCollector.class);


    protected Map<String, List<BlockingQueue<Tuple>>> _outgoingStreams;

    protected Map<String, MkTupleParam> _outgoingTupleParams;


    public TestOutputCollector()
    {
        super(new IOutputCollector()
        {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple)
            {
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple)
            {

            }

            @Override
            public void ack(Tuple input)
            {

            }

            @Override
            public void fail(Tuple input)
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

    @Override
    public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple)
    {
        // Don't know what to do with this one


        return null;
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
    public List<Integer> emit(String streamId, List<Object> tuple)
    {
        // Put tuple in appropriate outgoing streams
        List<BlockingQueue<Tuple>> streams = _outgoingStreams.get(streamId);

        if (streams == null)
        {
            // No stream attached.. just return
            LOG.info("Tuple emitted on stream " + streamId + " but no components attached");

        } else
        {
            for(BlockingQueue<Tuple> stream : streams)
            {
                stream.add(Testing.testTuple(tuple, _outgoingTupleParams.get(streamId)));
            }
        }

        return null;
    }

    @Override
    public List<Integer> emit(List<Object> tuple)
    {
        emit(Utils.DEFAULT_STREAM_ID, tuple);

        return null;
    }

    @Override
    public List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple)
    {
        // Don't do anything with anchors
        return emit(streamId, tuple);
    }

    @Override
    public void ack(Tuple input)
    {
        // Not sure what to do here
    }

    @Override
    public void fail(Tuple input)
    {
        throw new AssertionError("Tuple failed");
    }
}
