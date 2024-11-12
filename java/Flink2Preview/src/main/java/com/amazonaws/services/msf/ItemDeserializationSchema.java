package com.amazonaws.services.msf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ItemDeserializationSchema implements DeserializationSchema<Item> {


    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long serialVersionUID = 3203704281187526717L;

    @Override
    public Item deserialize(byte[] bytes) throws IOException {
        return MAPPER.readValue(bytes, Item.class);
    }

    @Override
    public boolean isEndOfStream(Item item) {
        return false;
    }

    @Override
    public TypeInformation<Item> getProducedType() {
        return TypeInformation.of(Item.class);
    }
}
