/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mio;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Deserializer;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.JoinWindows;

import com.fasterxml.jackson.databind.JsonNode;

//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Kogentix
 */
public class App {

    /**
     * Tuple for a category and its associated number of qant.
     */
    private static final class CatsWithQants {

        private final long category;
        private final long qant;

        public CatsWithQants(long category, long qant) {

            if (qant < 0) {
                throw new IllegalArgumentException("qant must not be negative");
            }
            this.category = category;
            this.qant = qant;
        }

        public long getCat() {
            return category;
        }

        public long getQant() {
            return qant;
        }

    }

    public static void main(String[] args) throws Exception {

        final int port = 1234;
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "mio");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        //streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);

        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + port);

        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<Integer> intSerde = Serdes.Integer();
        final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();

        //Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
        //GenericRecord record = new GenericData.Record(schema);
        // In the subsequent lines we define the processing topology of the Streams application.
        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, GenericRecord> kstrItem = builder.stream("mio-jdbc-Item");

        KStream<Long, Long> kstrItem1 = kstrItem.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("itemId").toString()), Long.parseLong(record.get("categoryId").toString())));

//        //write stream to intermediate topic
//        kstrItem1.to(longSerde, longSerde, "item-table-topic");
//
//        //Read back from the topic
//        KTable<Long, Long> ktabItem = builder.table(longSerde, longSerde, "item-table-topic", "item-store");


        KTable<Long, Long> ktabItem = kstrItem1.groupByKey(longSerde, longSerde)
                .reduce((aggValue, newValue) -> newValue, "item-store");

        //ktabItem.print(longSerde, longSerde, "Item");
        //Create Customer Table
        KStream<String, GenericRecord> kstrCustomer = builder.stream("mio-jdbc-Customer");

        KStream<Long, String> kstrCust = kstrCustomer.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("customerId").toString()), record.get("name").toString()));

        KTable<Long, String> ktabCust = kstrCust.groupByKey(longSerde, stringSerde)
                .reduce((aggValue, newValue) -> newValue, "customer-store");

        //ktabCust.print(longSerde, stringSerde, "Customer");
        //Create Category Table
        KStream<String, GenericRecord> kstrCategory = builder.stream("mio-jdbc-Category");

        KStream<Long, String> kstrCat = kstrCategory.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("categoryId").toString()), record.get("name").toString()));

        KTable<Long, String> ktabCat = kstrCat.groupByKey(longSerde, stringSerde)
                .reduce((aggValue, newValue) -> newValue, "category-store");

        //ktabCat.print(longSerde, stringSerde, "Category");
        //PE - rePlenishmentEvent
        KStream<String, GenericRecord> kstrPE = builder.stream("mio-jdbc-ReplenishmentEvent");

        KStream<Long, Long> kstrPEItemQant = kstrPE.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("itemId").toString()), Long.parseLong(record.get("quantity").toString())));

        //SE - SaleEvent
        KStream<String, GenericRecord> kstrSE = builder.stream("mio-jdbc-SaleEvent");

        KStream<Long, Long> kstrSEItemQant = kstrSE.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("itemId").toString()), Long.parseLong(record.get("quantity").toString())));

        KStream<Long, Long> kstrSECustQant = kstrSE.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("customerId").toString()), Long.parseLong(record.get("quantity").toString())));

        //RE - ReturnEvent
        KStream<String, GenericRecord> kstrReturnEvent = builder.stream("mio-jdbc-ReturnEvent");

        KStream<Long, Long> kstrREItemQant = kstrReturnEvent.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("itemId").toString()), Long.parseLong(record.get("quantity").toString())));

        KStream<Long, Long> kstrRECustQant = kstrReturnEvent.map((dummy, record)
                -> new KeyValue<>(Long.parseLong(record.get("customerId").toString()), Long.parseLong(record.get("quantity").toString())));

        //1. For each ItemId, the total number of items replenished so far
//        KTable<Long, Long> ktabPEQuantByItem = kstrItem1.leftJoin(kstrPEItemQant, (cat, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, longSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "refill-qant-aggby-item-store");
        KTable<Long, Long> ktabPEQuantByItem = kstrPEItemQant.groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "refill-qty-by-item-store-0.8");

        KTable<Long, Long> ktabPEQuantByItem1 = ktabItem.leftJoin(ktabPEQuantByItem, (cat, quant) -> (quant == null ? 0 : quant));

        //Hack for issue https://issues.apache.org/jira/browse/KAFKA-4609
        ktabPEQuantByItem1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "refill-qty-by-item-store-0.9")
                .through(longSerde, longSerde, "refill-qty-by-item-topic", "refill-qty-by-item-store");

        //ktabPEQuantByItem1.print(longSerde, longSerde, "refill-qant-aggby-item");
        //2. For each ItemId, the total number of items purchased so far
//        KTable<Long, Long> ktabSEQuantByItem = kstrItem1.leftJoin(kstrSEItemQant, (cat, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, longSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "sale-qant-aggby-item-store");
        KTable<Long, Long> ktabSEQuantByItem = kstrSEItemQant.groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "sale-qty-by-item-store-0.8");

        KTable<Long, Long> ktabSEQuantByItem1 = ktabItem.leftJoin(ktabSEQuantByItem, (cat, quant) -> (quant == null ? 0 : quant));

        ktabSEQuantByItem1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "sale-qty-by-item-store-0.9")
                .through(longSerde, longSerde, "sale-qty-by-item-topic", "sale-qty-by-item-store");

        //ktabSEQuantByItem1.print(longSerde, longSerde, "sale-qant-aggby-item");
        //3. For each ItemId, the total number of items returned so far
//        KTable<Long, Long> ktabREQuantByItem = kstrItem1.leftJoin(kstrREItemQant, (cat, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, longSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "return-qant-aggby-item-store");
        KTable<Long, Long> ktabREQuantByItem = kstrREItemQant.groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "rets-qty-by-item-store-0.8");

        KTable<Long, Long> ktabREQuantByItem1 = ktabItem.leftJoin(ktabREQuantByItem, (cat, quant) -> (quant == null ? 0 : quant));

        ktabREQuantByItem1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "rets-qty-by-item-store-0.9")
                .through(longSerde, longSerde, "rets-qty-by-item-topic", "rets-qty-by-item-store");

        //ktabREQuantByItem1.print(longSerde, longSerde, "return-qant-aggby-item");
        //4. For each customerId, the total number of items purchased
//        KTable<Long, Long> ktabPEQuantByCust = kstrCust.leftJoin(kstrSEItemQant, (name, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, stringSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "sale-qant-aggby-cust-store");
        KTable<Long, Long> ktabSEQuantByCust = kstrSECustQant.groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "sale-qty-by-cust-store-0.8");

        KTable<Long, Long> ktabSEQuantByCust1 = ktabCust.leftJoin(ktabSEQuantByCust, (name, quant) -> (quant == null ? 0 : quant));

        ktabSEQuantByCust1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "sale-qty-by-cust-store-0.9")
                .through(longSerde, longSerde, "sale-qty-by-cust-topic", "sale-qty-by-cust-store");

        //ktabSEQuantByCust1.print(longSerde, longSerde, "sale-qant-aggby-cust");
        //5. For each customerId, the total number of items returned
//        KTable<Long, Long> ktabREQuantByCust = kstrCust.leftJoin(kstrREItemQant, (name, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, stringSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "return-qant-aggby-cust-store");
        KTable<Long, Long> ktabREQuantByCust = kstrRECustQant.groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "rets-qty-by-cust-store-0.8");

        KTable<Long, Long> ktabREQuantByCust1 = ktabCust.leftJoin(ktabREQuantByCust, (name, quant) -> (quant == null ? 0 : quant));

        ktabREQuantByCust1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "rets-qty-by-cust-store-0.9")
                .through(longSerde, longSerde, "rets-qty-by-cust-topic", "rets-qty-by-cust-store");

        //ktabREQuantByCust1.print(longSerde, longSerde, "return-qant-aggby-cust");
        //6. For each product categoryId, total number of items replenished
//        KTable<Long, Long> ktabPEQuantByCat = kstrCat.leftJoin(kstrPEItemQant, (cat, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, stringSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "refill-qant-aggby-ctgry-store");
        KTable<Long, Long> ktabPEQantByCat = kstrPEItemQant.leftJoin(ktabItem, (qant, cat) -> new CatsWithQants(cat == null ? 999999 : cat, qant), longSerde, longSerde)
                .map((itemId, kwq) -> new KeyValue<>(kwq.getCat(), kwq.getQant()))
                .groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "refill-qty-by-cat-store-0.8");
        //In case, items 123, 124, 125, belonging to categoryId 1000 are not refilled and categoryId 1000 consists of only types 123, 124, 125.
        KTable<Long, Long> ktabPEQantByCat1 = ktabCust.leftJoin(ktabPEQantByCat, (name, quant) -> (quant == null ? 0 : quant));

        ktabPEQantByCat1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "refill-qty-by-cat-store-0.9")
                .through(longSerde, longSerde, "refill-qty-by-cat-topic", "refill-qty-by-cat-store");

        //ktabPEQantByCat1.print(longSerde, longSerde, "refil-qant-aggby-cat");
        //7. For each product categoryId, total number of items purchased
//        KTable<Long, Long> ktabSEQuantByCat = kstrCat.leftJoin(kstrSEItemQant, (cat, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, stringSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "sale-qant-aggby-ctgry-store");
        KTable<Long, Long> ktabSEQantByCat = kstrSEItemQant.leftJoin(ktabItem, (qat, cat) -> new CatsWithQants(cat == null ? 999999 : cat, qat), longSerde, longSerde)
                .map((itemId, kwq) -> new KeyValue<>(kwq.getCat(), kwq.getQant()))
                .groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "sale-qty-by-cat-store-0.8");

        KTable<Long, Long> ktabSEQantByCat1 = ktabCat.leftJoin(ktabSEQantByCat, (name, quant) -> (quant == null ? 0 : quant));
        ktabSEQantByCat1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "sale-qty-by-cat-store-0.9")
                .through(longSerde, longSerde, "sale-qty-by-cat-topic", "sale-qty-by-cat-store");

        //ktabSEQantByCat1.print(longSerde, longSerde, "sale-qant-aggby-cat");
        //8. For each product categoryId, total number of items returned
//        KTable<Long, Long> ktabREQuantByCat = kstrCat.leftJoin(kstrREItemQant, (cat, quant) -> (quant == null ? 0 : quant),
//                JoinWindows.of(TimeUnit.MINUTES.toMillis(50)), longSerde, stringSerde, longSerde)
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "return-qant-aggby-ctgry-store");
        KTable<Long, Long> ktabREQantByCat = kstrREItemQant.leftJoin(ktabItem, (qant, cat) -> new CatsWithQants(cat == null ? 999999 : cat, qant), longSerde, longSerde)
                .map((itemId, kwq) -> new KeyValue<>(kwq.getCat(), kwq.getQant()))
                .groupByKey(longSerde, longSerde)
                .reduce((aggv, curv) -> aggv + curv, "rets-qty-by-cat-store-0.8");
        //In case, items 123, 124, 125, belonging to categoryId 1000 are not retuned and categoryId 1000 consists of only types 123, 124, 125.
        KTable<Long, Long> ktabREQantByCat1 = ktabCust.leftJoin(ktabREQantByCat, (name, quant) -> (quant == null ? 0 : quant));

        ktabREQantByCat1.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "rets-qty-by-cat-store-0.9")
                .through(longSerde, longSerde, "rets-qty-by-cat-topic", "rets-qty-by-cat-store");

        //ktabREQantByCat1.print(longSerde, longSerde, "return-qant-aggby-cat");
        //9. For each ItemId, the current number of inventory items at hand considering purchases, returns and replenishments.
        //1 - 2 + 3
        KTable<Long, Long> ktabRepSale = ktabPEQuantByItem1.leftJoin(ktabSEQuantByItem1,
                (repQuant, saleQuant) -> (repQuant - (saleQuant == null ? 0 : saleQuant)));
        KTable<Long, Long> ktabTotItemsInStock = ktabRepSale.leftJoin(ktabREQuantByItem1,
                (repsaleQuant, retQuant) -> (repsaleQuant + (retQuant == null ? 0 : retQuant)));

        ktabTotItemsInStock.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "items-at-hand-store-0.9")
                .through(longSerde, longSerde, "items-at-hand-topic", "items-at-hand-store");
        //ktabTotItemsInStock.print(longSerde, longSerde, "TotItemsInStock");

        //10. For each customerId, the total number of items in possession considering purchases and returns
        //4 - 5
        KTable<Long, Long> ktabTotItemsWithCust = ktabSEQuantByCust1.leftJoin(ktabREQuantByCust1,
                (saleQuant, retQuant) -> (saleQuant - (retQuant == null ? 0 : retQuant)));

        ktabTotItemsWithCust.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "items-with-cust-store-0.9")
                .through(longSerde, longSerde, "items-with-cust-topic", "items-with-cust-store");

        //ktabTotItemsWithCust.print(longSerde, longSerde, "TotItemsWithCust");
        //11. For each product categoryId, total number of items at hand considering purchases, returns and replenishments.
        //6 - 7 + 8
        KTable<Long, Long> ktabRepSaleCat = ktabPEQantByCat1.leftJoin(ktabSEQantByCat1,
                (repQuant, saleQuant) -> (repQuant - (saleQuant == null ? 0 : saleQuant)));
        KTable<Long, Long> ktabTotItemsPerCatInStock = ktabRepSaleCat.leftJoin(ktabREQantByCat1,
                (repsaleQuant, retQuant) -> (repsaleQuant + (retQuant == null ? 0 : retQuant)));

        ktabTotItemsPerCatInStock.groupBy((key, value) -> KeyValue.pair(key, value), longSerde, longSerde)
                .reduce((agg, nvalue) -> nvalue, (agg, ovalue) -> ovalue, "items-at-hand-per-cat-0.9")
                .through(longSerde, longSerde, "items-at-hand-per-cat-topic", "items-at-hand-per-cat-store");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.cleanUp();
        streams.start();
        final RestService restService = startRestProxy(streams, port);

        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.cleanUp();
                streams.close();
                restService.stop();
            } catch (Exception e) {
                // ignored
            }
        }));
    }

    static RestService startRestProxy(final KafkaStreams streams, final int port)
            throws Exception {
        final RestService restService = new RestService(streams);
        restService.start(port);
        return restService;
    }

}

//ktabTotItemsPerCatInStock.print(longSerde, longSerde, "TotItemsPerCatInStock");
//        //SaleEvent(SE) aggregation
//        //Aggregation by itemID - For each ItemId, the total number of items purchased so far.
//        //Aggregation by customerID - For each customerId, the total number of items purchased.
////        KTable<Long, Long> ktabSEItemQant = kstrSEItemQant.groupByKey(longSerde, longSerde)
////                .aggregate(
////                        () -> 0L,
////                        (aggKey, newValue, aggValue) -> aggValue + newValue,
////                        Serdes.Long(),
////                        "se-qant-aggby-item-store");
//        KTable<Long, Long> ktabSECustQant = kstrSECustQant.groupByKey(longSerde, longSerde)
//                .aggregate(
//                        () -> 0L,
//                        (aggKey, newValue, aggValue) -> aggValue + newValue,
//                        Serdes.Long(),
//                        "se-qant-aggby-cust-store");
//
//        //ReturnEvent(RE) aggregation
//        //Aggregation by itemID - For each ItemId, the total number of items returned so far.
//        //Aggregation by customerID - For each customerId, the total number of items returned.
//        KTable<Long, Long> ktabREItemQant = kstrREItemQant.groupByKey(longSerde, longSerde)
//                .aggregate(
//                        () -> 0L,
//                        (aggKey, newValue, aggValue) -> aggValue + newValue,
//                        Serdes.Long(),
//                        "re-qant-aggby-item-store");
//
////        KTable<Long, Long> ktabRECustQant = kstrRECustQant.groupByKey(longSerde, longSerde)
////                .aggregate(
////                        () -> 0L,
////                        (aggKey, newValue, aggValue) -> aggValue + newValue,
////                        Serdes.Long(),
////                        "re-qant-aggby-cust-store");
//        //ReplenishmentEvent(PE) aggregation
//        //Aggregation by itemID - For each ItemId, the total number of items replenished so far.
//        KTable<Long, Long> ktabPEItemQant = kstrPEItemQant.groupByKey(longSerde, longSerde)
//                .aggregate(
//                        () -> 0L,
//                        (aggKey, newValue, aggValue) -> aggValue + newValue,
//                        Serdes.Long(),
//                        "pe-qant-aggby-item-store");
//
//        //Left join: SaleEvent -> Item
//        //For each product categoryId, total number of items purchased
////        KTable<Long, Long> ktabCatSaleQat = kstrSEItemQant.leftJoin(ktabItem, (qat, cat) -> new CatsWithQants(cat == null ? 999999 : cat, qat), longSerde, longSerde)
////                .map((itemId, kwq) -> new KeyValue<>(kwq.getCat(), kwq.getQant()))
////                .groupByKey(longSerde, longSerde)
////                .reduce((aggv, curv) -> aggv + curv, "se-qant-aggby-cat-store");
//        //Left join: ReturnEvent -> Item
//        //For each product categoryId, total number of items returned
//        KTable<Long, Long> ktabCatRetQat = kstrREItemQant.leftJoin(ktabItem, (qat, cat) -> new CatsWithQants(cat == null ? 999999 : cat, qat), longSerde, longSerde)
//                .map((itemId, kwq) -> new KeyValue<>(kwq.getCat(), kwq.getQant()))
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "re-qant-aggby-cat-store");
//
//        //Left join: ReplenishmentEvent -> Item
//        //For each product categoryId, total number of items replenished
//        KTable<Long, Long> ktabCatRepQat = kstrPEItemQant.leftJoin(ktabItem, (qat, cat) -> new CatsWithQants(cat == null ? 999999 : cat, qat), longSerde, longSerde)
//                .map((itemId, kwq) -> new KeyValue<>(kwq.getCat(), kwq.getQant()))
//                .groupByKey(longSerde, longSerde)
//                .reduce((aggv, curv) -> aggv + curv, "pe-qant-aggby-cat-store");
//
//        //For each ItemId, the current number of inventory items at hand considering purchases, returns and replenishments.
//        //rep+ret-sales
//        //ktabPEItemQant + ktabREItemQant
//        //Join tables
//        //ktabPEItemQant.print(longSerde, longSerde, "PEItemQant");
//        KTable<Long, Long> ktabRep = ktabItem.leftJoin(ktabPEItemQant, (cat, quant) -> (quant == null ? 0 : quant));
//
//        //ktabRep.print(longSerde, longSerde, "ItemRep");
//        //KTable<Long, Long> ktabRepSale = ktabRep.leftJoin(ktabSEItemQant, (repQuant, saleQuant) -> (repQuant - (saleQuant == null ? 0 : saleQuant)));
//        //ktabRepSale.print(longSerde, longSerde, "ItemRepSale");
//        //KTable<Long, Long> ktabTotItemsInStock = ktabRepSale.leftJoin(ktabREItemQant, (repsaleQuant, retQuant) -> (repsaleQuant + (retQuant == null ? 0 : retQuant)));
//        //ktabTotItemsInStock.print(longSerde, longSerde, "ItemRepSaleRet");
//        //For each customerId, the total number of items in possession considering purchases and returns - SaleEvent, ReturnEvent
//        //total purchased items per customerID - total items returned per customerId
//        KTable<Long, Long> ktabRECustQant = kstrRECustQant.groupByKey(longSerde, longSerde)
//                .aggregate(
//                        () -> 0L,
//                        (aggKey, newValue, aggValue) -> aggValue + newValue,
//                        Serdes.Long(),
//                        "re-qant-aggby-cust-store");
//
//        KTable<Long, Long> ktabTotItemsPerCust = ktabSECustQant.leftJoin(ktabRECustQant, (purQuant, retQuant) -> purQuant - (retQuant == null ? 0 : retQuant));
//
//        //For each product categoryId, total number of items at hand considering purchases, returns and replenishments.
//        //tot reps per catID + tot rets per catID - tot sales per catID
//        //KTable<Long, Long> ktabRepRetPerCat = ktabCatRepQat.leftJoin(ktabCatRetQat, (repQuant, retQuant) -> (repQuant == null ? 0 : repQuant) + (retQuant == null ? 0 : retQuant));
//        //KTable<Long, Long> ktabTotItemsInStockPerCat = ktabRepRetPerCat.leftJoin(ktabCatSaleQat, (repretQuant, saleQuant) -> (repretQuant) - (saleQuant == null ? 0 : repretQuant));
////        KTable<Long, Long> ktabTotItemsInStockPerCat = ktabCatRepQat.leftJoin(ktabCatRetQat, (repQuant, retQuant) -> (repQuant == null ? 0 : repQuant) + (retQuant == null ? 0 : retQuant))
////                                                      .leftJoin(ktabCatSaleQat, (repretQuant, saleQuant) -> (repretQuant) - (saleQuant == null ? 0 : repretQuant));
////        ktabTotItemsInStock.print(longSerde, longSerde, "TotItemsInStock");
//        // ktabTotItemsPerCust.print(longSerde, longSerde, "TotItemsWithCust");
//        // ktabTotItemsInStockPerCat.print(longSerde, longSerde, "TotItemsInStockPerCat");
