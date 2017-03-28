package com.ndustrialio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.ndustrialio.database.query.Query;
import com.ndustrialio.database.query.Select;
import com.ndustrialio.storm.spout.BaseNdustrialioSpout;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmhunt on 12/11/16.
 */
public abstract class DatabaseSpout extends BaseNdustrialioSpout
{
    protected Query _query;

    protected SpoutOutputCollector _collector;

    protected int _listIndex = 0;

    protected List<List<Object>> _rows;


    public DatabaseSpout(Query query)
    {
        if (!(query instanceof Select))
        {
            throw new RuntimeException("Must instantiate DatabaseSpout with a SELECT query!");
        }

        _query = query;
        _rows = new ArrayList<>();
    }


    @Override
    public void nextTuple()
    {

        _collector.emit(new Values(_rows.get(_listIndex).toArray()));

        _listIndex++;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        List<String> columns = _query.columns();

        declarer.declare(new Fields(columns.toArray(new String[columns.size()])));
    }


}
