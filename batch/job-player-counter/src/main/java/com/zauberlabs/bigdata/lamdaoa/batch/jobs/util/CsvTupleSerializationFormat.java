/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.jobs.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

/**
 * Tuples are represented:
 *          - with elements separated by commas
 *          - with values not surrounded by quotes
 * 
 * @since Jul 5, 2013
 */
public class CsvTupleSerializationFormat implements TupleSerializationFormat {

    /** @see TupleSerializationFormat#serialize(String[]) */
    @Override
    public String serialize(String[] tuple) {
        Validate.notNull(tuple);
        return StringUtils.join(tuple, ',');
    }

    /** @see TupleSerializationFormat#deSerialize(String) */
    @Override
    public String[] deSerialize(String serializedTuple) {
        Validate.notNull(serializedTuple);
        Validate.notEmpty(serializedTuple);
        return StringUtils.split(serializedTuple, ',');
    }

}
