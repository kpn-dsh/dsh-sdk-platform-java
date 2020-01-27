package dsh.kafka;

import dsh.pki.Pki;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class KafkaParser {
    private static final Logger logger = LoggerFactory.getLogger(KafkaParser.class);

    /**
     *
     * @param pki  The 'Pki' object used to handshake with the platform
     * @return KafkaParser object with helper functionality to access all Kafka related resources.
     */
    public static KafkaParser parse(Pki pki) { return new KafkaParser(validate(pki)); }

    private static Pki validate(Pki pki) {
        //TODO: checks ...
        return pki;
    }

    private final Pki pki;
    private KafkaParser(Pki pki) { this.pki = pki; }

    /** */
    private Properties addAllKafkaSsl(Properties props) {
        Properties allProps = new Properties();
        allProps.putAll(props);

        allProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        allProps.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, pki.getPassword());
        allProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, pki.getTruststoreFile().getAbsolutePath());
        allProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, pki.getPassword());
        allProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, pki.getKeystoreFile().getAbsolutePath());
        allProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, pki.getPassword());

        logger.debug("Kafka SSL properties added: {}", allProps.stringPropertyNames());
        return allProps;
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
        Properties filtered = addAllKafkaSsl(new Properties());
        filtered.putAll(
                ProducerConfig.configNames().stream()
                        .filter(pki.getProps()::containsKey)
                        .sorted()
                        .collect(Collectors.toMap(
                                s -> s, pki.getProps()::getProperty,
                                (e1, e2) -> e2,
                                LinkedHashMap::new
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
        Properties filtered = addAllKafkaSsl(new Properties());
        filtered.putAll(
                ConsumerConfig.configNames().stream()
                        .filter(pki.getProps()::containsKey)
                        .sorted()
                        .collect(Collectors.toMap(
                                s -> s, pki.getProps()::getProperty,
                                (e1, e2) -> e2,
                                LinkedHashMap::new
                        ))
        );

        if(overrides != null) filtered.putAll(overrides);
        return filtered;
    }

    /** */
    public String takeConsumerGroup(ConsumerGroupType typ) {
        return allConsumerGroups(typ).stream().sorted().findFirst().orElseThrow(NoSuchElementException::new);
    }

    /** */
    public Set<String> allConsumerGroups(ConsumerGroupType typ) {
        try {
            switch (typ) {
                case PRIVATE: return new HashSet<>(Arrays.asList(pki.getProps().getProperty("consumerGroups.private").split(",")));
                case SHARED:  return new HashSet<>(Arrays.asList(pki.getProps().getProperty("consumerGroups.shared").split(",")));
                default: throw new IllegalArgumentException("Invalid consumergroup type");
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid consumergroup config", e);
        }
    }
}
