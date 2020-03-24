# dsh-sdk-platform-java

|                                  |   |  |
| -------------------------------- |---|:---------------:|
| ![Java Sdk](pics/logo.png)       |   | <big><big>DSH</big> Java SDK</big><br><br>_**High Performance**_ / _**High Volume**_<br>_**Easy Usage**_</big>


## Getting started

### Introduction

The DSH Java Platform SDK is a set of Java classes and functions that make it easy to deploy containers and applications on the DSH.
It abstracts out all the required actions that are needed to authenticate and configure against the policies of the DSH.

### Design

The communication with the platform PKI component is done in a _lazy_ way.
This means that connecting to it will only be done when needed.
1) kafka properties are fetched only when you actually access them trough the `KafkaParser` API.
2) Stream contracts and configuration is only fetched when they are indeed queried from the application. 

## APIs

The entire SDK consists out of 3 parts :
- SDK initialization on the platform
- Kafka specific configuration and factory functions
- DSH Streams configuration and helper functions

In order to initialize itself, the SDK might setup connections to the platform PKI service, or configure itself using an already
available configuration file.  Pre-existing configuration can be passed to the JVM by the system property `platform.properties.file` which contains
all the relevant properties for the SDK to configure itself from.

When no properties file is provided, the SDK will fetch all required configuration from the platform PKI service.  This is done in a lazy way, 
so only when e.g. Kafka configuration is actually used (through the `KafkaConfigParser`) it is queried from the PKI service.

### SDK API

The Sdk has a built in `Builder` class to easily create a new Sdk object from any given starting situation.

#### initializing from environment variables

The most used initialization for stand-alone applications on the DSH will be performed by initializaing the SDK from the environment variables
given to the application by the platform.

In order for your application to be able to get configured through the SDK, it needs following environment variables to be provided by the DSH:

| variable                  | mandatory? | description
|:--------------------------|:----------:|:------------- 
|`DSH_SECRET_TOKEN`         | yes        | The DSH _secret_ needed to register the application with the PKI service.
|`DSH_CA_CERTIFICATE`       | yes        | The DSH specific CA Certificate to sign against.
|`KAFKA_CONFIG_HOST`        | yes        | The PKI service connection string
|`MESOS_TASK_ID`            | yes        | The task ID assigned to the application instance
|`MARATHON_APP_ID`          | yes        | The unique application ID for your application
|`DSH_CONTAINER_DNS_NAME`   | yes        | When serving, the DNS name

Just call `new Sdk.Builder().fromEnv().build()` to create a new fully initialized, ready to use Sdk object. 

#### manual initialization

As the SDK has a Builder to fully configure it, this Builder can be used to create a new Sdk object from scratch, or override certain properties.
See `dsh.sdk.Sdk.Builder` for a detailed explanation of the individual configuration properties available.

#### automatic initialization

_Automatic initialization_ basically tries to find out whether ta pre-existing configuration file is present (through the system variable `platform.properties.file`) or not.
When this configuration file is detected, all required initialization values for the SDK are read from this file, so there is no need anymore to contact the
external PKI service of the platform.  When no such file is detected, the SDK assumes the PKI service needs to be contacted and will configure itself as such.

This configuration option might come in handy for application running on pre-configured frameworks running in the tenant network.
Most of the time these frameworks already have a PKI configuration sitting somewhere locally that can be re-used for your application, and if not, you 
will fallback to the default of using the environment variables given by the platform to contact the PKI service.   

To auto-initialize the SDK, simply call `new Sdk.Builder().autoDetect().build()`

### Kafka Config Parser

From a initialized Sdk object, you can create a `KafkaConfigParser` object to access all required (and needed) Kafka configuration to run on the DSH.
Simply create it using your `Sdk` object: `KafkaConfigParser.of(sdk)` or create one from pre-existing properties: `KafkaConfigParser.of(properties)`.
Keep in mind that creating it directly from Properties requires knowledge about the parser itself and the way the properties' key and values are encoded.

|     |     |
| --- | --- |
| ![Info](pics/info64.png) | In practice you will most of the time use the Sdk object to create the `KafkaConfigParser` from.  
 
#### Kafka Serdes

The SDK comes with the needed Serializers and Deserializers to handle the mandatory envelopes on the _public_ streams on DSH.
The `kafka.serdes` can be directly configured in the Kafka consumer/producer properties, and the basically wrap the general purpose
serialize- and deserialize functions provided by the `dsh.messages.Serdes` class.

##### DataEnvelope serializer/deserializer

Kafka serializer and deserializer for the DataEnvelopes.

##### KeyEnvelope serializer/deserializer

Kafka serializer and deserializer for the KeyEnvelopes.

#### Kafka Partitioners

Kafka configurable partitioners

##### DshStreamPartitioner

The `DshStreamPartitioner` class describes the logic of _general purpose_ partitioning functionality,
that makes use of the _stream contracts_ of the streams the kafka topics you produce on belong to.

Basically it keeps a cache of the topics your producer produces on with the required partitioning schema for that topic,
derived from the stream contracts through the configured `StreamsConfigParser`.

The main partitioning function `public int partition(String topic, String key, int numPartitions, BiFunction<String, Integer, Integer> defaultPartitioner)`
will fetch the required partitioning schema for the topic from the _StreamsConfigParser_ and cache the partitioning function for quick lookup.  When no contract
is found for the topic, it will use the provided `defaultPartitioner` function.

##### DynamicStreamPartitioner

The `DynamicStreamPartitioner` makes use of the `DshStreamPartitioner` functionality to automatically configure
the partitioning function depending on which topic is produced to.

Basically the logic looks like this:
1. When the k_key_ is a _KeyEnvelope_, extract the inner key as data to partition on.
2. When not a _KeyEnvelope_, use the raw bytes of the key
3. Look for a partitioning function for the given topic
   Look up the configured partitioning schema in the stream-contract trough the `StreamsConfigParser`
4. use the provided _fallback partitioning function_ when none found in step 3.
5. calculate partition trough the selected partitioning function

This intelligent partitioner can be configured through Kafka config properties:
`properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DynamicStreamPartitioner.class.getName())`
 
|     |     |
| --- | --- |
| ![Info](pics/info64.png) | The `DynamicStreamPartitioner` needs to be configured with a valid `StreamConfigParser` object as follows: `properties.put(DynamicStreamPartitioner.CONFIG_KEY, streamsParser);`

### Streams Config Parser

From a initialized Sdk object, you can create a `StreamsConfigParser` object to access all stream meta data (like the stream contract, the partitioning schema,
the read/write topic names, ...)

Simply create it using your `Sdk` object: `StreamsConfigParser.of(sdk)` or create one from pre-existing properties: `StreamsConfigParser.of(properties)`.
Keep in mind that creating it directly from Properties requires knowledge about the parser itself and the way the properties' key and values are encoded.

|     |     |
| --- | --- |
| ![Info](pics/info64.png) | In practice you will most of the time use the Sdk object to create the `StreamsConfigParser` from.  
 
#### Datastreams

The `DataStream` class represents a abstract DSH stream.
Most of the SDK functions operate on this _abstract stream_ level, as it makes little sense to act on individual Kafka topics while the DSH 
handles all data on _streams_.  Two types of steams exist: _Internal_ and _Public_.

The most common used textual representation of a stream would be `internal:<stream-name>` for **internal** streams, and `stream:<stream-name>` for **public** streams.
The `DataStream` class provides functions to convert to/from Strings and true DataStream objects for usage within the `StreamsConfigParser`.

### KafkaClientFactory

The `KafkaClientFactory`, as the name already suggests, provides functionality to easily create new Kafka producers and/or consumer
that is already fully configured for use on the DSH and ready to produce/consume to/from any stream.

The `KafkaClientFactory` basically makes use of an existing `KafkaConfigParser` and `StreamsConfigParser` object to handle the most common
configuration actions required for a Kafka Consumer and/or Producer.

### Simple REST server

A very basic HTTP server is included in the SDK that can be used to return a health status or metrics.  

```
   boolean isHealth()      { ... }
   String  scrapeMetrics() { ... }
 
   Service myServer = new SimpleRestServer.Builder()
                           .setListener("/health", () -> this.isHealthy())
                           .setListener("/metrics", () -> this.scrapeMetrics())
 
   myServer.start();
```

## Usage

To initialize the SDK
```
Sdk sdk = new Sdk.Builder().autoDetect().build()
```
> This will automatically detect if the SDK needs to be initialized from 'scratch' trough environment variables given by the DSH to the container,
>or if it can be initialized from an existing configuration file that exists on the nodes
>(and is passed trough the system property `platform.properties.file`).


from here on the `sdk` object can be used to initialize the parsers
1) to access Kafka specific configuration:
    ```
    KafkaConfigParser kp = KafkaConfigParser.of(sdk)
    ```
2) to access stream specific config:
    ```
    StreamsConfigParser sp = StreamsConfigParser.of(sdk)
    ```

### Standalone App

The example blow describes a simple skeleton for an application on the DSH

```
  Sdk sdk = new Sdk.Builder().autoDetect().build();
  KafkaConfigParser   kafkaParser   = KafkaConfigParser.of(sdk);
  StreamsConfigParser streamsParser = StreamsConfigParser.of(sdk);
  KafkaClientFactory  clientFactory = new KafkaClientFactory(streamsParser, kafkaParser);


  // create a new kafka producer
  KafkaProducer[KeyEnvelope, DataEnvelope] producer = clientFactory.createStreamProducer(null);

  // create a new kafka consumer
  // that will be part of the same consumergroup as other instances of this application will be.
  KafkaConsumer[KeyEnvelope, DataEnvelope] consumer = clientFactory.createSharedStreamConsumer(null);
  
  List<DataStream> myStreams = ...  // streams to consume the data from
  consumer.subscribe(subscriptionPatternFor(myStreams));

  for(;;) {
    records = consumer.poll(Integer.MAX_VALUE);
    . . .
  }

```

### Frameworks (Spark, Flink)

The problem with frameworks like Apache Flink, Apache Spark is that they read in the application (job), construct all classes needed, and then
serialize it to send it over to worker instances that will execute the software.
This requires all objects to be _serializable_ to be recreated on the workers.  For the SDK this is not the case.
The SDK itself, together with the _parsers_ can not be serialized.  It needs to be re-created on the worker instances.
This is not necessarily a problem, but something to take into account, otherwise you will run into exceptions like "NotSerializableException"

From the `KafkaConfigParser`, `StreamsConfigParser`, `Serdes` and `Partitioners` available in the SDK you have all building blocks available for
integrating into basically any framework that uses Kafka for processing.

#### Flink Job skeleton example
 
As Flink serializes the JAR and sends it over to the workers, the best approach is to make sure all config
resides in a _singleton object_.  Keep in mind that in this case the Job will be initialized from the configuration
available on the worker instances of the Flink cluster.

```
// ---
// Scala config object skeleton example
// ---
object JobConfig {

  @transient private lazy val sdk: Sdk = new Sdk.Builder().autoDetect().build()
  @transient private lazy val kafkaParser = KafkaConfigParser.of(sdk)
  @transient private lazy val streamParser = StreamsConfigParser.of(sdk)

  def producerConfig: Properties = kafkaParser.kafkaProducerProperties(null)

  def consumerConfig: Properties = KafkaConfigParser.addConsumerGroup(
    kafkaParser.suggestedConsumerGroup(ConsumerGroupType.SHARED),
    kafkaParser.kafkaConsumerProperties(null))

  def identity: Envelope.Identity = sdk.getApp.identity()

  . . .
}
``` 

## Building the SDK

### local deployment

`mvn clean install`
> This will build, test, create the JavaDocs and install the SDK in your local Maven repo

### making a new release

`mvn clean deploy`
> Make sure the version in the `pom.xml` file is updated correctly.

[logo] icons/logo.png