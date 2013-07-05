/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.playercounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Sums over tuples of the format <fragger, fragCount>
 * 
 * 
 * @since Jul 5, 2013
 */
public class PlayerFragCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /** @see Reducer#reduce(Object, Iterable, Reducer.Context) */
    @Override
    protected void reduce(Text word, Iterable<IntWritable> partialCounts, 
                            Reducer<Text, IntWritable, Text, IntWritable>.Context ctx)
                            throws IOException, InterruptedException {
        int aggregatedFrags = 0;
        for (IntWritable partialCount : partialCounts) {
            aggregatedFrags += partialCount.get();
        }
        ctx.write(word, new IntWritable(aggregatedFrags));
    }
    
}
