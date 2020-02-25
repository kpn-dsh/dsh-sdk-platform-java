package dsh.kafka;

import dsh.sdk.Sdk;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static dsh.kafka.KafkaConfig.CONSUMERGROUP_PRIVATE_CONFIG;
import static dsh.kafka.KafkaConfig.CONSUMERGROUP_SHARED_CONFIG;

/**
 *
 */
public class KafkaParser {
    private static final Logger logger = LoggerFactory.getLogger(KafkaParser.class);

    /**
     *
     * @param sdk  The 'Pki' object used to handshake with the platform
     * @return KafkaParser object with helper functionality to access all Kafka related resources.
     */
    public static KafkaParser of(Sdk sdk) {
        return new KafkaParser(addAllKafkaSsl(sdk, sdk.getProps()));
    }
    public static KafkaParser of(Properties props) { return new KafkaParser(props); }
    public static KafkaParser of(Map<String, ?> map) {
        Properties props = new Properties();
        props.putAll(map);
        return new KafkaParser(props);
    }

    private final Properties kafkaProps = new Properties();
    private final Map<ConsumerGroupType, List<String>> suggestedCg = new HashMap<>();

    private KafkaParser(Properties props) {
        kafkaProps.putAll(
        props.keySet().stream()
                .filter(k -> ProducerConfig.configNames().contains(k.toString()) || ConsumerConfig.configNames().contains(k.toString()))
                .collect(Collectors.toMap(
                        e -> e,
                        props::get
                ))
        );

        suggestedCg.put(ConsumerGroupType.PRIVATE, Arrays.asList(props.getProperty(CONSUMERGROUP_PRIVATE_CONFIG).split(",")));
        suggestedCg.put(ConsumerGroupType.SHARED, Arrays.asList(props.getProperty(CONSUMERGROUP_SHARED_CONFIG).split(",")));
    }

    /** */
    private static Properties addAllKafkaSsl(Sdk sdk, Properties baseProps) {
        Properties props = new Properties();
        props.putAll(baseProps);
        props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.putIfAbsent(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sdk.getPki().getPassword());
        props.putIfAbsent(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sdk.getPki().getTruststoreFile().getAbsolutePath());
        props.putIfAbsent(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sdk.getPki().getPassword());
        props.putIfAbsent(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sdk.getPki().getTruststoreFile().getAbsolutePath());
        props.putIfAbsent(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sdk.getPki().getPassword());

        return props;
    }

    /**
     * Adds the given consumer group to the provided Kafka configuration properties
     *
     * <pre>
     *     // create Kafka consumer config -- run within own, application-private, consumer group
     *     Properties myConsumerProperties = parser.addConsumerGroup(
     *                                          parser.takeConsumerGroup(ConsumerGroupType.PRIVATE),
     *                                          parser.kafkaConsumerProperties(null)
     *                                       );
     * </pre>
     *
     * @param groupId
     * @param props
     * @return
     */
    public static Properties addConsumerGroup(String groupId, Properties props) {
        Properties allProps = new Properties();
        allProps.putAll(props);

        allProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return allProps;
    }

    /**
     * The consumer group type your application wants to work with.
     *
     * PRIVATE
     *   The consumer group identifier is application local, meaning that a consumergroup can only be formed within
     *   this application instance (e.g. when running multiple consumers in multiple threads)
     *
     * PUBLIC
     *   The consumer group identifier is application public, meaning that a consumergroup will be formed across all
     *   running instances of the application (when running distributed on multiple machines)
     */
    public enum ConsumerGroupType {
        PRIVATE,
        SHARED
    }

    /**
     * Gives the Kafka properties to use to initialize a new Producer,
     * extended with the provided custom properties.
     * All Kafka specific properties are fetched from the PKI properties.
     *
     * <pre>
     *     // create new Kafka producer
     *     Properties customProperties = new Properties();
     *     custom.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   , StringSerializer.class );
     *     custom.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class );
     *
     *     Producer p = new KafkaProducer( parser.kafkaProducerProperties(customProperties) );
     * </pre>
     *
     * @param overrides  custom properties that need to be included
     * @return  properties fir to create a new Kafka producer
     */
    public Properties kafkaProducerProperties(Properties overrides) {
        Properties filtered = new Properties();
        filtered.putAll(
                ProducerConfig.configNames().stream()
                        .filter(kafkaProps::containsKey)
                        .collect(Collectors.toMap(
                                k -> k,
                                kafkaProps::get
                        ))
        );

        if(overrides != null) filtered.putAll(overrides);
        return filtered;
    }

    /**
     * Gives the Kafka properties to use to initialize a new Consumer,
     * extended with the provided custom properties.
     * All Kafka specific properties are fetched from the PKI properties.
     *
     * <pre>
     *     // create new Kafka consumer
     *     Properties customProperties = new Properties();
     *     custom.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   , StringDeserializer.class );
     *     custom.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class );
     *
     *     Consumer c = new KafkaConsumer(
     *                          parser.addConsumerGroup(
     *                                  parser.takeConsumerGroup(ConsumerGroupType.PUBLIC),
     *                                  parser.kafkaConsumerProperties(customProperties)
     *                          )
     *                  );
     * </pre>
     *
     * @param overrides  custom properties that need to be included
     * @return  properties fit to create a new Kafka Consumer
     */
    public Properties kafkaConsumerProperties(Properties overrides) {
        Properties filtered = new Properties();
        filtered.putAll(
                ConsumerConfig.configNames().stream()
                        .filter(kafkaProps::containsKey)
                        .collect(Collectors.toMap(
                                k -> k,
                                kafkaProps::get
                        ))
        );

        if(overrides != null) filtered.putAll(overrides);
        return filtered;
    }

    /** */
    public String suggestedConsumerGroup(ConsumerGroupType typ) {
        return allConsumerGroups(typ).stream().sorted().findFirst().orElseThrow(NoSuchElementException::new);
    }

    /** */
    public List<String> allConsumerGroups(ConsumerGroupType typ) {
        return suggestedCg.getOrDefault(typ, Collections.emptyList());
    }
}
