/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.util;

/**
 * Defines how tuples will be serialized and parsed.
 * 
 * 
 * @since Jul 5, 2013
 */
public interface TupleSerializationFormat {
    
    /** serializes a tuple */
    String serialize(String[] tuple);

    /** interprets a tuple serialized with this format. */
    String[] deSerialize(String serializedTuple);
    
}
