package com.ndustrialio.storm.spout;

import java.io.IOException;
import java.util.*;

import com.ndustrialio.messaging.RabbitExchangeOptions;
import com.ndustrialio.messaging.RabbitQueueOptions;
import com.rabbitmq.client.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;

public abstract class BaseRabbitSpout extends BaseNdustrialioSpout
{
	

	public static final int MAX_FAILS = 30;
	public static final int PREFETCH_COUNT=5000;
	
	protected String _routingKey;
	protected SpoutOutputCollector _collector;
	protected QueueingConsumer _consumer;
	protected Channel _channel;

	protected List<Address> _rabbitHosts;
	protected String _rabbitUser;
	protected String _rabbitPassword;
	protected String _rabbitPrefix;

	protected int _prefetchCount=PREFETCH_COUNT;

    protected Logger LOG;

    // Generate our own message ID or let subclasses do it?
	protected boolean _generateMessageID = true;

    // Pull exchange and queue information from config?
    protected boolean _pullValues;

    // Declare exchange?
    protected boolean _declareExchange;

    // Log failures?
    protected boolean _logFails = false;

    // Queue and exchange options
    protected RabbitQueueOptions _queueOptions;
    protected RabbitExchangeOptions _exchangeOptions;


	protected HashMap<Object,Long> deliveryTagHash = new HashMap<Object,Long>();

	public BaseRabbitSpout(Class loggerClass)
	{
        // Will get values from configuration on spout open
		_pullValues = true;
        _exchangeOptions=null;
        _queueOptions=null;

        LOG = LoggerFactory.getLogger(loggerClass);

    }

	public BaseRabbitSpout(RabbitQueueOptions queueOptions, String exchangeName, String routingKey, Class loggerClass)
	{
        _pullValues = false;
		_routingKey = routingKey;
		_exchangeOptions = new RabbitExchangeOptions(exchangeName);
		_queueOptions = queueOptions;

        _declareExchange=false;

        LOG = LoggerFactory.getLogger(loggerClass);


    }

    public BaseRabbitSpout(RabbitQueueOptions queueOptions, RabbitExchangeOptions exchangeOptions, String routingKey, Class loggerClass)
    {
        _pullValues = false;
        _routingKey = routingKey;
        _exchangeOptions = exchangeOptions;
        _queueOptions = queueOptions;

        _declareExchange = true;

        LOG = LoggerFactory.getLogger(loggerClass);

    }

	public BaseRabbitSpout setMessageIDGeneration(boolean generateMessageID)
	{
		_generateMessageID = generateMessageID;

		return this;
	}

	public BaseRabbitSpout setPrefetchCount(int prefetchCount)
	{
		_prefetchCount = prefetchCount;

		return this;
	}

    public BaseRabbitSpout setFailLogging(boolean logFails)
    {
        _logFails = logFails;

        return this;
    }

	
	public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

	@Override
	public void nextTuple()
	{
		QueueingConsumer.Delivery delivery = null;
		try {
			delivery = _consumer.nextDelivery(200);
		} catch (ShutdownSignalException|ConsumerCancelledException|InterruptedException e) {
			LOG.error("Rabbit consume failed: " + e.getMessage());
			System.exit(1);
		}
		
		if (delivery == null)
		{
			Utils.sleep(50);
		} else
		{
			String msg = new String(delivery.getBody());
			
			JSONObject obj = new JSONObject(msg);

            Object msg_id;

            // Generate message ID if needed
            // The onNextTuple method will return the message ID
            // that we are to store in the hash.  It may be the same as the one provided.
            if (_generateMessageID)
            {
                msg_id = onNextTuple(obj,
                        UUID.randomUUID(),
                        delivery.getEnvelope().getRoutingKey());
            } else
            {
                msg_id = onNextTuple(obj,
                        null,
                        delivery.getEnvelope().getRoutingKey());
            }

			if (msg_id != null)
			{
				deliveryTagHash.put(msg_id, delivery.getEnvelope().getDeliveryTag());

			} else
			{
				try
				{
					_channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				} catch (IOException e)
				{
					LOG.error("Error acking rejected message");
					e.printStackTrace();
				}
			}
			
		}
	}

    @Override
	public void ack(Object id) {
		try {
			long deliveryTag = deliveryTagHash.remove(id);
			//LOG.info("Message acked, unacked: " + deliveryTagHash.size());

			_channel.basicAck(deliveryTag, false);

            onTupleAck(id);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException npe) {
			LOG.error(npe.toString(),npe);
		}
	}




	@Override
	public void fail(Object id) {
		try
        {
			long deliveryTag = deliveryTagHash.remove(id);
            if (_logFails)
            {
                LOG.error("Message failed. Msg_id: " + id.toString() + " Delivery tag: " + deliveryTag);
            }
			_channel.basicReject(deliveryTag, true);

            onTupleFail(id);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector)
	{
		setConf(conf);

        onPreOpen(conf, context, collector);

		_rabbitHosts = new ArrayList<>();
		
		try
        {
			try
			{
				JSONArray hosts = new JSONArray((String) getConfigurationValue("rabbit_hosts"));

				for(int i = 0; i < hosts.length(); i++)
				{
					_rabbitHosts.add(new Address(hosts.getString(i)));
				}

			} catch (RuntimeException e)
			{
				_rabbitHosts.add(new Address((String) getConfigurationValue("rabbit_host")));
			}
            _rabbitUser = (String) this.getConfigurationValue("rabbit_user");
            _rabbitPassword = (String) this.getConfigurationValue("rabbit_pass");

			if (_pullValues)
            {
			_rabbitPrefix = (String) this.getConfigurationValue("rabbit_prefix");
			_exchangeOptions = new RabbitExchangeOptions((String)this.getConfigurationValue("rabbit_exchange"));
			_routingKey = (String) this.getConfigurationValue("rabbit_routing_key");
			_queueOptions = new RabbitQueueOptions((String) this.getConfigurationValue("rabbit_queue"));
			}

		} catch (RuntimeException e)
        {
			_collector.reportError(e);
			throw e;
		}

        // Apply prefix?
        // TODO: impossible to use _pullValues and a null prefix due to exception
		if (_rabbitPrefix != null)
		{
			_exchangeOptions.name=  _rabbitPrefix + "." + _exchangeOptions.name;
			_queueOptions.name = _rabbitPrefix + "." + _queueOptions.name;
			_routingKey = _rabbitPrefix + "." + _routingKey;
		}

		_collector = collector;

		LOG.info("BaseRabbitSpout created");
		LOG.info("exchange name: " + _exchangeOptions.name);
		LOG.info("queue name: " + _queueOptions.name);
		LOG.info("routing key: " + _routingKey);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(_rabbitUser);
		factory.setPassword(_rabbitPassword);
		// Enable automatic recovery
		factory.setAutomaticRecoveryEnabled(true);

		try
		{
			// Connect with list of hosts
			Connection connection =
					factory.newConnection(_rabbitHosts.toArray(new Address[_rabbitHosts.size()]));
			_channel = connection.createChannel();
			if (_declareExchange)
			{
				_channel.exchangeDeclare(_exchangeOptions.name,
                        _exchangeOptions.type,
                        _exchangeOptions.durable,
                        _exchangeOptions.autoDelete,
                        _exchangeOptions.options);
			}
			_channel.queueDeclare(_queueOptions.name,
                    _queueOptions.durable,
                    _queueOptions.exclusive,
                    _queueOptions.autoDelete,
                    _queueOptions.options);

            // Bind queue and exchange with routing key
			_channel.queueBind(_queueOptions.name, _exchangeOptions.name, _routingKey);

			_consumer = new QueueingConsumer(_channel);
			_channel.basicQos(_prefetchCount);

            // Begin consumption from queue
			_channel.basicConsume(_queueOptions.name, false, _consumer);

		}
		catch (IOException e)
		{
			LOG.error(e.toString(), e);
			_collector.reportError(e);
			throw new RuntimeException(e.getMessage());
		}

		onPostOpen(conf, context, collector);


	}

    // TODO: We can probably de-generify this now that onNextTuple is a RabbitSpout method
	public abstract Object onNextTuple(JSONObject rabbitMessage, Object msg_id, String routingKey);



}
