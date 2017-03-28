package com.ndustrialio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.ndustrialio.database.PostgresClient;
import com.ndustrialio.database.QueryResponse;
import com.ndustrialio.database.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 12/7/16.
 */
public class PostgresSpout extends DatabaseSpout
{

    public static final String COMPONENT_ID = PostgresSpout.class.getSimpleName();



    public PostgresSpout(Query query)
    {

        super(query);

    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        PostgresClient client = new PostgresClient(conf, PostgresClient.CONNECTION_TYPE.SIMPLE);

        // Use try block to ensure that resources are closed appropriately
        try(QueryResponse queryResult = client.executeSelect(_query))
        {
            ResultSet rs = queryResult.getResults();

            while(rs.next())
            {
                List<Object> l = new ArrayList<>();
                _rows.add(l);
                for(String column : _query.columns())
                {
                    l.add(rs.getObject(column));
                }
            }
        } catch(SQLException e)
        {
            throw new RuntimeException("Error executing query: " + e.getMessage());
        }

        _collector = collector;
    }



}
