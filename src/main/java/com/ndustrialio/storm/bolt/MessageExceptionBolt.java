package com.ndustrialio.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ndustrialio.database.JDBCClient;
import com.ndustrialio.database.PostgresClient;
import com.ndustrialio.database.column.DatabaseColumn;
import com.ndustrialio.database.DatabaseModel;
import com.ndustrialio.database.column.SerialIdColumn;
import com.ndustrialio.database.column.TimestampColumn;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmhunt on 4/3/16.
 */
public class MessageExceptionBolt extends BaseNdustrialioBolt
{

    public static final String COMPONENT_ID = "MessageExceptionBolt";

    private final static Logger LOG = LoggerFactory.getLogger(MessageExceptionBolt.class);

    public static Fields FIELDS = new Fields("msg_id", "topology", "bolt", "exception", "exception_time");


    private DatabaseModel _model;
    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        _collector = collector;
        try
        {
            _model = new DatabaseModel(new PostgresClient(stormConf, JDBCClient.CONNECTION_TYPE.POOLED),
                    "message_exceptions",
                    Arrays.asList(new SerialIdColumn(),
                            new DatabaseColumn("msg_id", DatabaseColumn.ColumnType.VARCHAR),
                            new DatabaseColumn("topology", DatabaseColumn.ColumnType.VARCHAR),
                            new DatabaseColumn("bolt", DatabaseColumn.ColumnType.VARCHAR),
                            new DatabaseColumn("exception_type", DatabaseColumn.ColumnType.VARCHAR),
                            new DatabaseColumn("exception_message", DatabaseColumn.ColumnType.VARCHAR),
                            new TimestampColumn("exception_time"),
                            new TimestampColumn("created_at").setDefaultNow(),
                            new TimestampColumn("updated_at").disableInsert()),
                    null,
                    null,
                    null);
        } catch (SQLException e)
        {
            _collector.reportError(e);
        }

    }

    @Override
    public void execute(Tuple tuple)
    {
        try
        {
            createNewMessageException(
                    (String)tuple.getValueByField("msg_id"),
                    (String)tuple.getValueByField("topology"),
                    (String)tuple.getValueByField("bolt"),
                    (Exception)tuple.getValueByField("exception"),
                    (DateTime)tuple.getValueByField("exception_time"));

            _collector.ack(tuple);
        } catch(Exception e)
        {
            LOG.error("Error creating message exception: " +
                    stackTraceToString(e) + " tuple: " + tuple.toString());
            _collector.ack(tuple);

        }
    }

    public void createNewMessageException(String msg_id,
                                          String topology, String bolt,
                                          Exception exception, DateTime exception_time) throws Exception

    {
        Map<String, Object> newMessageException = new HashMap<>();

        newMessageException.put("msg_id", msg_id);
        newMessageException.put("topology", topology);
        newMessageException.put("bolt", bolt);
        newMessageException.put("exception_type", exception.getClass().getSimpleName());
        newMessageException.put("exception_message", exception.getMessage());
        newMessageException.put("exception_time", exception_time.toString("yyyy-MM-dd HH:mm:ss.SSS"));


        _model.insert(newMessageException);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // No emissions from here
    }
}
