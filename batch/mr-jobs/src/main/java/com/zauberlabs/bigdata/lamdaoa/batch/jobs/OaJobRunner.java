/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.zauberlabs.bigdata.lamdaoa.batch.jobs.playercounter.PlayerFragCountMapper;
import com.zauberlabs.bigdata.lamdaoa.batch.jobs.playercounter.PlayerFragCountReducer;
import com.zauberlabs.bigdata.lamdaoa.batch.jobs.vscounter.PlayerVsCountMapper;
import com.zauberlabs.bigdata.lamdaoa.batch.jobs.vscounter.PlayerVsCountReducer;

/**
 * Main class for map reduce job.  
 * 
 * 
 * Run using:
 * 
 *  hadoop jar lambdaoa-batch-mr-jobs.jar <job-name> /user/hadoop/input/ /user/hadoop/output/
 * 
 * where <job-name> is "player-frag-count" or "player-vs-count".
 * 
 * 
 * @since Jul 5, 2013
 */
public class OaJobRunner {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        
        job.setJarByClass(OaJobRunner.class);
        
        String jobName = args[0];
        
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + jobName));
        
        job.setJobName(jobName);
        if ("player-frag-count".equals(jobName)) {
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            job.setMapperClass(PlayerFragCountMapper.class);
            job.setCombinerClass(PlayerFragCountReducer.class);
            job.setReducerClass(PlayerFragCountReducer.class);
        } else if ("player-vs-count".equals(jobName)) {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            job.setMapperClass(PlayerVsCountMapper.class);
            job.setCombinerClass(PlayerVsCountReducer.class);
            job.setReducerClass(PlayerVsCountReducer.class);    
            
        }
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
