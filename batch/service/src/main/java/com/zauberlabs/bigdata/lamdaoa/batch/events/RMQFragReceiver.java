/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.events;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.zauberlabs.bigdata.lamdaoa.batch.Frag;

/**
 * Processes events from a RabbitMQ queue indefinitely.
 * 
 * 
 * @since Aug 23, 2013
 */
public class RMQFragReceiver implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final QueueingConsumer consumer;
    private final FragListener listener;

    public RMQFragReceiver(QueueingConsumer consumer, FragListener listener) {
        Validate.notNull(consumer);
        Validate.notNull(listener);
        this.consumer = consumer;
        this.listener = listener;
    }
    
    
    /** @see java.lang.Runnable#run() */
    @Override
    public void run() {
        while(true) {
            try {
                Delivery delivery = consumer.nextDelivery();
                Frag frag = parse(delivery);
                listener.process(frag);
            } catch (Exception e) {
                // Exceptions do not stop the processing thread.
                // They are considered a circumstantial error of processing one message.
                logger.error("Error reading event from RMQ", e);
            }

        }
    }

    private Frag parse(Delivery delivery) {
        String msgString = new String(delivery.getBody());
        Validate.notNull(msgString, "empty payload");
        
        Date timestamp = timestamp(delivery);
        Validate.notNull(timestamp, "no timestamp for event");
        
        String[] fields = StringUtils.split(msgString, ",");
        Validate.isTrue(fields.length == 3, "invalid payload, expected 3 fields but got " + fields.length);
        
        return new Frag(timestamp, fields[0], fields[1], fields[2]);
    }
    
    private Date timestamp(Delivery delivery) {
        return DateUtils.truncate(delivery.getProperties().getTimestamp(), Calendar.MINUTE);
    }

}
