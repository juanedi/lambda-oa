/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.events;

import org.apache.commons.lang.UnhandledException;
import org.apache.commons.lang.Validate;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Creates the RMQ Consumer for frag events queue.
 * 
 * The queue is asumed to be bound to a "frag" exchange.
 * 
 * 
 * @since Aug 23, 2013
 */
public final class RMQBootstrapHelper {

    private final String host;
    
    private RMQBootstrapHelper(String host) {
        Validate.notEmpty(host);
        this.host = host;
    }

    public QueueingConsumer createFragConsumer() {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(host);
            Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            
            channel.exchangeDeclare("frags", "fanout", true, false, null);
            
            final String queueName = channel.queueDeclare().getQueue();
            
            channel.queueBind(queueName, "frags", "");
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, false, consumer);
            
            return consumer;
        } catch (Exception e) {
            throw new UnhandledException(e);
        }

    }
    
    public static RMQBootstrapHelper forHost(String host) {
        return new RMQBootstrapHelper(host);
    }
    
}
