/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.spouts;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.UnhandledException;
import org.apache.commons.lang.time.DateUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.base.Preconditions;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Rabbitmq feeder  
 * 
 * @since 05/07/2013
 */
@SuppressWarnings({"rawtypes", "serial"})
public class RabbitmqSpout extends BaseRichSpout {
    
    private SpoutOutputCollector collector;
    private QueueingConsumer consumer;
    private Channel channel;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.collector = Preconditions.checkNotNull(collector);
            
            final ConnectionFactory factory = new ConnectionFactory();
            
            factory.setHost("localhost");
            
            Connection connection = factory.newConnection();
            
            channel = connection.createChannel();
            
            channel.exchangeDeclare("frags", "fanout", true, false, null);
            
            final String queueName = channel.queueDeclare().getQueue();
            
            channel.queueBind(queueName, "frags", "");
            consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, false, consumer);
        } catch (Exception e) {
            throw new UnhandledException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            final QueueingConsumer.Delivery delivery = consumer.nextDelivery(1000);
            if (delivery != null) {
                final Long messageId = delivery.getEnvelope().getDeliveryTag();
                final String payload = new String(delivery.getBody());
                final Date date = DateUtils.truncate(delivery.getProperties().getTimestamp(), Calendar.MINUTE);
                collector.emit(new Values(date.getTime(), payload), messageId);
            }
        } catch (Exception e) {
            throw new UnhandledException(e);
        }
    }
    
    @Override
    public void ack(Object msgId) {
        try {
            final Long messageId = (Long) msgId;
            channel.basicAck(messageId, false);
            super.ack(msgId);
        } catch (Exception e) {
            throw new UnhandledException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time_frame", "record"));
    }

    
}
