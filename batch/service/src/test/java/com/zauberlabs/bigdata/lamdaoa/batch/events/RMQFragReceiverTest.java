/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.events;

import org.junit.Ignore;
import org.junit.Test;

import com.rabbitmq.client.QueueingConsumer;
import com.zauberlabs.bigdata.lamdaoa.batch.Frag;


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

    /**
     * {@link FragListener} that prints frag information.  
     * 
     * @since Aug 23, 2013
     */
    private static class PrintFragListener implements FragListener {

        /** @see FragListener#process(Frag) */
        @Override
        public void process(Frag frag) {
            System.out.println(frag.getTimestamp() + "," + frag.getGame()
                       + "," + frag.getFragger() + "," + frag.getFragged());
        }
        
    }
    
}
