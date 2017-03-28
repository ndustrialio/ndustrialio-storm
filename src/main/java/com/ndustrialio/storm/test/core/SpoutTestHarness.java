package com.ndustrialio.storm.test.core;

import backtype.storm.Config;
import backtype.storm.Testing;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ndustrialio.storm.spout.BaseNdustrialioSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

import static org.mockito.Mockito.mock;

/**
 * Created by jmhunt on 12/2/16.
 */
public class SpoutTestHarness extends ComponentTestHarness implements Runnable
{
    protected BaseNdustrialioSpout _spout;

    private final static Logger LOG = LoggerFactory.getLogger(SpoutTestHarness.class);


    protected TestSpoutOutputCollector _outputCollector;



    public SpoutTestHarness(BaseNdustrialioSpout spout, String componentID)
    {

        super(componentID);

        _spout = spout;

        _outputCollector = new TestSpoutOutputCollector();

        // Let this spout declare its outputs
        _spout.declareOutputFields(_declarer);



    }

    @Override
    public void setOutgoingStream(String streamId, BlockingQueue<Tuple> stream)
    {
        Fields declaredStream = _declarer.getDeclaredStreams().get(streamId);

        if (declaredStream == null)
        {
            throw new IllegalArgumentException("Attempted to link un-declared outgoing stream "
                                            + streamId + " from compoenent " + getComponentID());
        }

        // Create mock tuple parameters and pass them to the output collector
        // along with the stream itself.  Note that the stream is actually
        // The incoming stream of another harness
        MkTupleParam param = new MkTupleParam();

        param.setFields((String[])declaredStream.toList().toArray(new String[declaredStream.size()]));
        param.setComponent(getComponentID());
        param.setStream(streamId);

        _outputCollector.addOutgoingStream(streamId, param, stream);
    }


    public void open(Config config)
    {
        _spout.open(config, mock(TopologyContext.class), _outputCollector);
    }


    @Override
    public BlockingQueue<Tuple> getIncomingStream()
    {
        return null; //No incoming stream for a spout
    }

    @Override
    public void run()
    {
        LOG.info(_spout.getClass().getSimpleName() + ": spout harness starting up...");

        while(!Thread.currentThread().isInterrupted())
        {
            // Call spout nextTuple() in a tight loop
            try
            {
                _spout.nextTuple();
            } catch(Exception e)
            {
                // Interrupt thread
                LOG.info("Caught exception: " + e.getClass().getSimpleName() + " from spout, shutting down!");
                break;
            }
        }

        // Interrupted.. let's shut down.  Send poison pill.
        MkTupleParam param = new MkTupleParam();

        param.setFields("SHUTDOWN");
        param.setComponent(_spout.getClass().getSimpleName());

        _outputCollector.sendShutdown(Testing.testTuple(Arrays.asList("SHUTDOWN"), param));


    }
}
