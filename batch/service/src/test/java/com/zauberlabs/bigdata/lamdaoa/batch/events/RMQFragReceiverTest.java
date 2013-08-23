/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.events;

import org.junit.Ignore;
import org.junit.Test;

import com.rabbitmq.client.QueueingConsumer;


/**
 * Use to launch a {@link RMQFragReceiver}.
 * 
 * 
 * @since Aug 23, 2013
 */
@Ignore
public class RMQFragReceiverTest {

    @Test
    public void testName() throws Exception {
        PrintFragListener fragListener = new PrintFragListener();
        QueueingConsumer consumer = RMQBootstrapHelper.forHost("localhost").createFragConsumer();
        
        RMQFragReceiver receiver = new RMQFragReceiver(consumer, fragListener);
        receiver.run();
    }
    
}
