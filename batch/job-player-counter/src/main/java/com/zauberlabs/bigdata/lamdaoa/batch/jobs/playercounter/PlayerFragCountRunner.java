/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.playercounter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Main class for map reduce job.  
 * 
 * 
 * Run using:
 * 
 *  hadoop jar lambdaoa-batch-job-player-counter.jar /user/hadoop/input/ /user/hadoop/output/
 * 
 * 
 * @since Jul 5, 2013
 */
public class PlayerFragCountRunner {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        
        job.setJarByClass(PlayerFragCountRunner.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setJobName("player-frag-counter");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(PlayerFragCountMapper.class);
        job.setCombinerClass(PlayerFragCountReducer.class);
        job.setReducerClass(PlayerFragCountReducer.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
