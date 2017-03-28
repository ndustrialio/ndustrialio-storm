package com.ndustrialio.storm.test.core;

import backtype.storm.Config;
import backtype.storm.Testing;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkTupleParam;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ndustrialio.storm.bolt.BaseNdustrialioBolt;
import com.ndustrialio.utils.StatCollector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.mock;

/**
 * Created by jmhunt on 12/1/16.
 */
public class BoltTestHarness extends ComponentTestHarness implements Runnable
{

    private final static Logger LOG = LoggerFactory.getLogger(BoltTestHarness.class);



    protected BaseNdustrialioBolt _bolt;

    protected BlockingQueue<Tuple> _incomingStream;

    protected TestOutputCollector _outputCollector;

    protected StatCollector _profileStats = null;


    public BoltTestHarness(BaseNdustrialioBolt bolt, String componentID)
    {
        super(componentID);

        _incomingStream = new LinkedBlockingQueue<>();

        _bolt = bolt;
        _outputCollector = new TestOutputCollector();

        // Let this bolt declare its outputs
        _bolt.declareOutputFields(_declarer);


    }

    public void setProfilingEnabled()
    {
        _profileStats = new StatCollector();
    }


    public void prepare(Config config)
    {
        _bolt.prepare(config, mock(TopologyContext.class), _outputCollector);
    }

    @Override
    public BlockingQueue<Tuple> getIncomingStream()
    {
        return _incomingStream;
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

    @Override
    public void run()
    {
        LOG.info(_bolt.getClass().getSimpleName() + ": bolt harness starting up...");

        while(true)
        {
            try
            {
                Tuple tuple = _incomingStream.take();

                if (tuple.contains("SHUTDOWN"))
                {
                    if (tuple.getStringByField("SHUTDOWN").equals("SHUTDOWN"))
                    {

                        LOG.warn("Bolt " + _bolt.getClass().getSimpleName() + " shutting down!");

                        // Got shutdown poison-pill.. inform downstream bolts
                        // and shutdown ourselves
                        MkTupleParam param = new MkTupleParam();

                        param.setFields("SHUTDOWN");
                        param.setComponent(_bolt.getClass().getSimpleName());

                        _outputCollector.sendShutdown(Testing.testTuple(Arrays.asList("SHUTDOWN"), param));

                        break;
                    }
                }

                if (_profileStats != null)
                {
                    long startTime = System.currentTimeMillis();
                    _bolt.execute(tuple);
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    _profileStats.update(DateTime.now(), (double)elapsedTime);
                } else
                {
                    _bolt.execute(tuple);

                }

            } catch (InterruptedException e)
            {
                // Interrupted while waiting on tuple
                break;
            }


        }

        if (_profileStats != null)
        {
            LOG.info("Profiling results for " +_bolt.getClass().getSimpleName()
                    + "(ms): " + _profileStats.toString());
        }
    }

}
