package com.ndustrialio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.ndustrialio.database.CassandraUtility;
import com.ndustrialio.database.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 12/12/16.
 */
public class CassandraSpout extends DatabaseSpout
{
    public static final String COMPONENT_ID = CassandraSpout.class.getSimpleName();

    public CassandraSpout(Query query)
    {
        super(query);
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        CassandraUtility cassandra = new CassandraUtility(conf);

        PreparedStatement statement = cassandra.prepareStatement(_query.toString());


        BoundStatement bs = statement.bind(_query.arguments(0).toArray());


        ResultSet results = cassandra.executeSelect(bs);


        for (Row row : results)
        {
            List<Object> l = new ArrayList<>();
            _rows.add(l);
            for(String column : _query.columns())
            {
                l.add(row.getObject(column));
            }
        }

        _collector = collector;
    }
}
