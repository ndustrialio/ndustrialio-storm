package com.ndustrialio.storm.test;
import com.ndustrialio.storm.spout.RedisNotificationSpout;
import com.ndustrialio.storm.bolt.PrintBolt;
import com.ndustrialio.storm.test.core.TestTopologyBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by aaronnewman on 2/22/17.
 */
public class RedisTopologyTest {
    public static void main(String[] args) throws Exception
    {

        List<String> patterns = new ArrayList<String>();
        patterns.add("*");

        TestTopologyBuilder builder = new TestTopologyBuilder();

        builder.setSpout(RedisNotificationSpout.COMPONENT_ID, new RedisNotificationSpout(patterns, 0))
                .shuffleGrouping(PrintBolt.COMPONENT_ID);

        builder.setBolt(PrintBolt.COMPONENT_ID, new PrintBolt());

        Thread spoutThread = builder.submit();

        spoutThread.join();

    }
}
