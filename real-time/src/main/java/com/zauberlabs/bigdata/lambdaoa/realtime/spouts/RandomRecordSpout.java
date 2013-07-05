package com.zauberlabs.bigdata.lambdaoa.realtime.spouts;

import java.util.Date;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.collect.Iterables;

/**
 * Spout that emits random records 
 * 
 * 
 * @since 05/07/2013
 */
public class RandomRecordSpout extends BaseRichSpout {

    /** <code>serialVersionUID</code> */
    private static final long serialVersionUID = 132128069852775102L;
    private SpoutOutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        final String[] sentences = new String[] {
            "20130510162904,132,j,batman",
            "20130510162904,135,El Eternestor,Tu vieja",
        };
        for (String sentence : Iterables.cycle(sentences)) {
            collector.emit(new Values(new Date(), sentence), sentence.hashCode());
            Utils.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time_frame", "record"));
    }
   
}