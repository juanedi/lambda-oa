/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.playercounter;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Test player stats map reduce job.
 * 
 * 
 * @since Jul 5, 2013
 */
public class PlayerFragCountJobTest {

    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setUp() {
        PlayerFragCountMapper mapper = new PlayerFragCountMapper();
        PlayerFragCountReducer reducer = new PlayerFragCountReducer();
        mapDriver = new MapDriver(mapper);
        reduceDriver = new ReduceDriver(reducer);
        mapReduceDriver = new MapReduceDriver(mapper, reducer);
      
    }

    @Test
    public void test_mapper() throws Exception {
        mapDriver.withInput(new LongWritable(23), new Text("game,player1,player2"));
        mapDriver.withOutput(new Text("player1"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void test_reducer() throws Exception {
        reduceDriver.withInput(new Text("player1"), Arrays.asList(new IntWritable(1), new IntWritable(2)));
        reduceDriver.withOutput(new Text("player1"), new IntWritable(3));
        reduceDriver.runTest();        
    }
 
    @Test
    public void test_map_reduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(23), new Text("game,player1,player2"))
                       .withInput(new LongWritable(23), new Text("game,player1,player3"))
                       .withInput(new LongWritable(23), new Text("game,player2,player3"));
        
        mapReduceDriver.withOutput(new Text("player1"), new IntWritable(2))
                       .withOutput(new Text("player2"), new IntWritable(1));
        
        mapReduceDriver.runTest();
    }
    
}
