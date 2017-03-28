package com.ndustrialio.storm.spout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.ndustrialio.database.RedisClient;
import com.ndustrialio.cache.JedisOperation;
import com.ndustrialio.storm.bolt.PrintBolt;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by aaronnewman on 2/22/17.
 */
public class RedisNotificationSpout extends BaseNdustrialioSpout
{

    public static final String COMPONENT_ID = RedisNotificationSpout.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(PrintBolt.class);

    private RedisClient _client;
    private SpoutOutputCollector _collector;
    private BlockingQueue<Values> _messageQueue;
    private String[] _patterns;
    private String[] _fields;
    private int _dbNumber;

    public RedisNotificationSpout(List<String> patterns, int dbNumber) {
        _patterns = patterns.toArray(new String[patterns.size()]);
        _messageQueue = new LinkedBlockingQueue<Values>();
        _fields = new String[]{"pattern", "channel", "message"};
        _dbNumber = dbNumber;
    }

    private void startListener() {
        _client.execute(new JedisOperation() {
            @Override
            public Object execute(Jedis jedis) {
                jedis.psubscribe(new JedisPubSub() {
                    @Override
                    public void onPSubscribe(String pattern, int subscribedChannels) {
                        LOG.info("onPSubscribe " + pattern + " " + subscribedChannels);
                    }
                    @Override
                    public void onPMessage(String pattern, String channel, String message) {
                        Values notification = new Values();
                        notification.addAll(Arrays.asList(pattern, channel, message));
                        _messageQueue.add(notification);
                    }
                },_patterns);
                return null;
            }
        });
    }

    @Override
    public void nextTuple() {
        try {
            Values v = _messageQueue.take();
            _collector.emit(v);
        } catch (InterruptedException e) {
            LOG.error("Could not emit keyspace notification from redis spout");
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _client = new RedisClient(conf, _dbNumber);
        _collector = collector;
        Thread listenerThread = new Thread() {
            public void run() {
                startListener();
            }
        };
        listenerThread.start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(_fields));
    }

}
