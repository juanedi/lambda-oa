/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch;

import com.rabbitmq.client.QueueingConsumer;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.zauberlabs.bigdata.lamdaoa.batch.events.PrintFragListener;
import com.zauberlabs.bigdata.lamdaoa.batch.events.RMQBootstrapHelper;
import com.zauberlabs.bigdata.lamdaoa.batch.events.RMQFragReceiver;

/**
 * Dropwizard service that bootstraps application context, metrics and monitoring for batch jobs processing.
 * 
 * 
 * @since Jul 5, 2013
 */
public class BatchJobsService extends Service<BatchJobServiceConfiguration> {

    public static void main(final String[] args) throws Exception {
        new BatchJobsService().run(args);
    }
    
    /** @see Service#initialize(Bootstrap) */
    @Override
    public void initialize(Bootstrap<BatchJobServiceConfiguration> bootstrap) {
        bootstrap.setName("lambda-oa-batch");
    }

    /** @see Service#run(Configuration, Environment) */
    @Override
    public void run(BatchJobServiceConfiguration configuration, Environment environment) throws Exception {
        startupRmqListener(configuration.getRmqHost());
        environment.addResource(new PingResource());
    }

    private void startupRmqListener(String host) {
        QueueingConsumer consumer = RMQBootstrapHelper.forHost(host).createFragConsumer();
        RMQFragReceiver rmqFragReceiver = new RMQFragReceiver(consumer, new PrintFragListener());
        new Thread(rmqFragReceiver).start();
    }

}
