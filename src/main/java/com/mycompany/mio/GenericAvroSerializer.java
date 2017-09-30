/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mio;

/**
 *
 * @author Kogentix
 */


import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericAvroSerializer implements Serializer<GenericRecord> {

    KafkaAvroSerializer inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public GenericAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    public GenericAvroSerializer(SchemaRegistryClient client) {
        inner = new KafkaAvroSerializer(client);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, GenericRecord record) {
        return inner.serialize(topic, record);
    }

    @Override
    public void close() {
        inner.close();
    }
}
