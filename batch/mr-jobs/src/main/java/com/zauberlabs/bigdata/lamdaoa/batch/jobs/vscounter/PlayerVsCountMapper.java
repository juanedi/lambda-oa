/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.vscounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zauberlabs.bigdata.lamdaoa.batch.jobs.util.CsvTupleSerializationFormat;
import com.zauberlabs.bigdata.lamdaoa.batch.jobs.util.TupleSerializationFormat;

/**
 * For each entry with format <gameid,fragger, fragged> produces:
 * 
 *  fragger, fragged    1, 0
 *  fragged, fragged    0, 1
 * 
 * 
 * @since Jul 5, 2013
 */
public class PlayerVsCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    private TupleSerializationFormat format = new CsvTupleSerializationFormat();
    
    @Override
    protected final void map(LongWritable key, Text value,
                               Mapper<LongWritable, Text, Text, Text>.Context ctx) 
                               throws IOException, InterruptedException {
        String[] tuple = format.deSerialize(value.toString());
        if (checkTuple(tuple)) {
            String fragger = tuple[1];
            String fragged = tuple[2];
            
            ctx.write(new Text(fragger + "," + fragged), new Text("1,0"));
            ctx.write(new Text(fragged + "," + fragger), new Text("0,1"));
        }
    }

    private boolean checkTuple(String[] tuple) {
        return tuple != null && tuple.length == 3 && noEmptyElements(tuple);
    }
    
    private boolean noEmptyElements(String[] array) {
        for (String string : array) {
            if (string == null) {
                return false;
            }
        }
        return true;
    }
    
}
