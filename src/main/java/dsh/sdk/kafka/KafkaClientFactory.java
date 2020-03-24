package dsh.sdk.kafka;

import dsh.messages.Envelope;
import dsh.sdk.kafka.partitioners.DynamicStreamPartitioner;
import dsh.sdk.kafka.serdes.DataEnvelopeDeserializer;
import dsh.sdk.kafka.serdes.DataEnvelopeSerializer;
import dsh.sdk.kafka.serdes.KeyEnvelopeDeserializer;
import dsh.sdk.kafka.serdes.KeyEnvelopeSerializer;
import dsh.sdk.streams.StreamsConfigParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Factory class to craete Kafka clients that are automatically configured
 * to be able to produce correctly to a 'stream' (KafkaProducer)
 * or can consume from a stream or a set of streams.
 *
 * The required Envelope serdes are automatically configured,
 * and a partitioner is put in place to handle produces to a stream.
 *
 * These clients can be used for the majority of use-cases where your application
 * wants to consume and/or produce data from/to a DSH stream.
 */
public class KafkaClientFactory {
    private final StreamsConfigParser streamsParser;
    private final KafkaConfigParser kafkaParser;

    private KafkaClientFactory() { throw new AssertionError(); }
    private KafkaClientFactory(StreamsConfigParser streamsParser, KafkaConfigParser kafkaParser) {
        this.streamsParser = streamsParser;
        this.kafkaParser = kafkaParser;
    }

    public static KafkaClientFactory of(StreamsConfigParser streamsParser, KafkaConfigParser kafkaParser) {
        return new KafkaClientFactory(streamsParser, kafkaParser);
    }

    /**
     * Create a default use-case kafka producer
     *
     * @param overrides custom producer configuration
     * @return fully configured kafka producer
     */
    public KafkaProducer<Envelope.KeyEnvelope, Envelope.DataEnvelope> createStreamProducer(Properties overrides) {
        Properties props = kafkaParser.kafkaProducerProperties(overrides);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DynamicStreamPartitioner.class.getName());
        props.put(DynamicStreamPartitioner.CONFIG_KEY, streamsParser);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KeyEnvelopeSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DataEnvelopeSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    /**
     * Create a default use-case kafka consumer, sitting in a shared consumer group
     *
     * @param overrides custom consumer configuration
     * @return fully configured kafka consumer
     */
    public KafkaConsumer<Envelope.KeyEnvelope, Envelope.DataEnvelope> createSharedStreamConsumer(Properties overrides) {
        Properties props = kafkaParser.kafkaConsumerProperties(overrides);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KeyEnvelopeDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DataEnvelopeDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, kafkaParser.suggestedConsumerGroup(KafkaConfigParser.ConsumerGroupType.SHARED));

        return new KafkaConsumer<>(props);
    }

    /**
     * Create a default use-case kafka consumer, sitting in a private consumer group
     *
     * @param overrides custom consumer configuration
     * @return fully configured kafka consumer
     */
    public KafkaConsumer<Envelope.KeyEnvelope, Envelope.DataEnvelope> createLocalStreamConsumer(Properties overrides) {
        Properties props = kafkaParser.kafkaConsumerProperties(overrides);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KeyEnvelopeDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DataEnvelopeDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, kafkaParser.suggestedConsumerGroup(KafkaConfigParser.ConsumerGroupType.PRIVATE));

        return new KafkaConsumer<>(props);
    }
}
