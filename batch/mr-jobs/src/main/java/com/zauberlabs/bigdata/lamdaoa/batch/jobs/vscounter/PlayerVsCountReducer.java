/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.vscounter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * For a pair <player1, player2> receives a list of <fragsP1, fragsP2> and sums over them.
 * 
 * 
 * @since Jul 5, 2013
 */
public class PlayerVsCountReducer extends Reducer<Text, Text, Text, Text> {

    /** @see Reducer#reduce(Object, Iterable, Reducer.Context) */
    @Override
    protected void reduce(Text key, Iterable<Text> partials, 
                            Reducer<Text, Text, Text, Text>.Context ctx)
                            throws IOException, InterruptedException {
        long aggregatedFragsP1 = 0;
        long aggregatedFragsP2 = 0;
        
        for (Text fragBalance : partials) {
            String[] fields = fragBalance.toString().split(",");
            Long fragsP1 = Long.valueOf(fields[0]);
            aggregatedFragsP1 += fragsP1;
            
            Long fragsP2 = Long.valueOf(fields[1]);
            aggregatedFragsP2 += fragsP2;
        }
        
        String output = aggregatedFragsP1 + "," + aggregatedFragsP2;
        ctx.write(key, new Text(output));
    }
    
}
