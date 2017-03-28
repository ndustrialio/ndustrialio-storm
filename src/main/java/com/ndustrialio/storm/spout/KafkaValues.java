package com.ndustrialio.storm.spout;


import backtype.storm.tuple.Values;

/**
 * Created by jmhunt on 3/17/17.
 */
public class KafkaValues extends Values
{
    // Emit these values on this stream
    protected String _streamID = "default";

    public KafkaValues emitOn(String streamID)
    {
        _streamID = streamID;

        return this;
    }

    public String getStreamID()
    {
        return _streamID;
    }

}
