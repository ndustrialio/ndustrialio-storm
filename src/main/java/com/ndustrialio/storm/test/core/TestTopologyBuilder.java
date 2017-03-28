package com.ndustrialio.storm.test.core;

import backtype.storm.Config;
import com.ndustrialio.core.TopologyConfiguration;
import com.ndustrialio.storm.bolt.BaseNdustrialioBolt;
import com.ndustrialio.storm.spout.BaseNdustrialioSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 12/2/16.
 */
public class TestTopologyBuilder
{

    public static final String TOPOLOGY_UUID = "50631116-5733-0160-73e4-8c0c04e3fa24";

    private List<ComponentLinker> _components;

    private Map<String, BoltTestHarness> _bolts;

    private Config _config;

    // Restrict to one spout, for now
    private SpoutTestHarness _spout;

    public TestTopologyBuilder()
    {
        _components = new ArrayList<>();
        _bolts = new HashMap<>();

        _config = getConfiguration();
    }


    public ComponentLinker setSpout(String componentID, BaseNdustrialioSpout spout)
    {
        _spout = new SpoutTestHarness(spout, componentID);

        ComponentLinker declarer = _spout.getComponentLinker(this);

        // Spout goes first in list
        _components.add(0, declarer);

        return declarer;
    }

    public void configure(String key, Object value)
    {
        // Override configuration value
        Map<String, Object> env = (Map<String, Object>)_config.get("topology.environment");

        env.put(key, value);
    }


    private Config getConfiguration()
    {
        Map<String, String> env = System.getenv();
        TopologyConfiguration apiConfig;

        try
        {
            // Get unit test config
            apiConfig = new TopologyConfiguration(TOPOLOGY_UUID,
                    "development", env.get("ACCESS_TOKEN"));
        } catch(Exception e)
        {
            throw new RuntimeException("Error getting config");
        }


        Config ret  = new Config();
        ret.setEnvironment(apiConfig.getConf());

        ret.setNumWorkers(20);
        ret.setMaxSpoutPending(1000);
        ret.setNumAckers(1);
        ret.setMessageTimeoutSecs(300);
        ret.setDebug(false);
        ret.put("topology_debug", false);

        return ret;
    }

    public ComponentLinker setBolt(String componentID, BaseNdustrialioBolt bolt)
    {
        return this.setBolt(componentID, bolt, false);
    }


    public ComponentLinker setBolt(String componentID, BaseNdustrialioBolt bolt, boolean profile)
    {
        BoltTestHarness harness = new BoltTestHarness(bolt, componentID);

        if (profile)
        {
            harness.setProfilingEnabled();
        }
        _bolts.put(componentID, harness);

        ComponentLinker declarer = harness.getComponentLinker(this);

        _components.add(declarer);

        return declarer;
    }

    public ComponentTestHarness getTestHarness(String componentID)
    {
        return _bolts.get(componentID);
    }

    public Thread submit()
    {
        // Let's do this. Link topology components
        for (ComponentLinker linker : _components)
        {
            linker.link();
        }

        // Prepare spout
        _spout.open(_config);

        // Create spout thread
        Thread spoutThread = new Thread(_spout);

        for(BoltTestHarness harness : _bolts.values())
        {
            // Prepare bolt
            harness.prepare(_config);

            // Create bolt thread
            Thread t = new Thread(harness);

            // start bolt thread
            t.start();
        }

        // Bolt threads should be waiting

        // Start spout thread
        spoutThread.start();


        return spoutThread;

    }











}
