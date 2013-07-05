/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.vscounter;

import java.util.Arrays;

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
public class PlayerVsCountJobTest {

    private MapDriver<Object, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setUp() {
        PlayerVsCountMapper mapper = new PlayerVsCountMapper();
        PlayerVsCountReducer reducer = new PlayerVsCountReducer();

        mapDriver = new MapDriver(mapper);
        reduceDriver = new ReduceDriver(reducer);
        mapReduceDriver = new MapReduceDriver(mapper, reducer);
      
    }

    @Test
    public void test_mapper() throws Exception {
        mapDriver.withInput(new LongWritable(23), new Text("game,player1,player2"));
        mapDriver.withOutput(new Text("player1,player2"), new Text("1,0"))
                 .withOutput(new Text("player2,player1"), new Text("0,1"));
        mapDriver.runTest();
    }

    @Test
    public void test_reducer() throws Exception {
        reduceDriver.withInput(new Text("player1,player2"), Arrays.asList(
            new Text("1,0"),
            new Text("2,5")
        ));
        reduceDriver.withOutput(new Text("player1,player2"), new Text("3,5"));
        reduceDriver.runTest();        
    }
 
    @Test
    public void test_map_reduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(23), new Text("game,player1,player2"))
                       .withInput(new LongWritable(23), new Text("game,player1,player2"))
                       .withInput(new LongWritable(23), new Text("game,player2,player1"))
                       .withInput(new LongWritable(23), new Text("game,player2,player3"))
                       .withInput(new LongWritable(23), new Text("game,player1,player3"));
        
        mapReduceDriver.withOutput(new Text("player1,player2"), new Text("2,1"))
                       .withOutput(new Text("player1,player3"), new Text("1,0"))
                       
                       .withOutput(new Text("player2,player1"), new Text("1,2"))
                       .withOutput(new Text("player2,player3"), new Text("1,0"))
                       
                       .withOutput(new Text("player3,player1"), new Text("0,1"))
                       .withOutput(new Text("player3,player2"), new Text("0,1"));
        
        mapReduceDriver.runTest();
    }
    
}
