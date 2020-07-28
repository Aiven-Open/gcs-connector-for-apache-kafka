/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.gcs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

// TODO refactor test to make it more readable
@Testcontainers
final class IntegrationTest {
    private static String gcsCredentialsPath;
    private static String gcsCredentialsJson;

    private static final String CONNECTOR_NAME = "aiven-gcs-sink-connector";
    private static final String TEST_TOPIC_0 = "test-topic-0";
    private static final String TEST_TOPIC_1 = "test-topic-1";

    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static String testBucketName;

    private static String gcsPrefix;

    private static BucketAccessor testBucketAccessor;

    private static File pluginDir;

    @Container
    private final KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;

    private ConnectRunner connectRunner;


    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        gcsCredentialsPath = System.getProperty("integration-test.gcs.credentials.path");
        gcsCredentialsJson = System.getProperty("integration-test.gcs.credentials.json");

        testBucketName = System.getProperty("integration-test.gcs.bucket");

        final Storage storage = StorageOptions.newBuilder()
                .setCredentials(GoogleCredentialsBuilder.build(gcsCredentialsPath, gcsCredentialsJson))
                .build()
                .getService();
        testBucketAccessor = new BucketAccessor(storage, testBucketName);
        testBucketAccessor.ensureWorking();

        gcsPrefix = "aiven-kafka-connect-gcs-test-"
                + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final File testDir = Files.createTempDirectory("aiven-kafka-connect-gcs-test-").toFile();

        pluginDir = new File(testDir, "plugins/aiven-kafka-connect-gcs/");
        assert pluginDir.mkdirs();

        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
                distFile.toString(), pluginDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBucketAccessor.clear(gcsPrefix);

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClient = AdminClient.create(adminClientConfig);

        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);

        final NewTopic newTopic0 = new NewTopic(TEST_TOPIC_0, 4, (short) 1);
        final NewTopic newTopic1 = new NewTopic(TEST_TOPIC_1, 4, (short) 1);
        adminClient.createTopics(Arrays.asList(newTopic0, newTopic1)).all().get();

        connectRunner = new ConnectRunner(pluginDir, kafka.getBootstrapServers(), OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        adminClient.close();
        producer.close();

        testBucketAccessor.clear(gcsPrefix);

        connectRunner.awaitStop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void basicTest(final String compression) throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                cnt += 1;

                sendFutures.add(sendMessageAsync(TEST_TOPIC_0, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0, compression),
                getBlobName(1, 0, compression),
                getBlobName(2, 0, compression),
                getBlobName(3, 0, compression));
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            blobContents.put(blobName,
                    testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1).stream()
                            .map(fields -> String.join(",", fields))
                            .collect(Collectors.toList())
            );
        }

        cnt = 0;
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                cnt += 1;

                final String blobName = getBlobName(partition, 0, compression);
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = key + "," + value;
                assertEquals(expectedLine, actualLine);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void groupByTimestampVariable(final String compression) throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put(
                "file.name.template",
                "{{topic}}-{{partition}}-{{start_offset}}-"
                        + "{{timestamp:unit=YYYY}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}"
        );
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 0, "key-0", "value-0"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 0, "key-1", "value-1"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 0, "key-2", "value-2"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 1, "key-3", "value-3"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 3, "key-4", "value-4"));

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final Map<String, String[]> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(
                getTimestampBlobName(0, 0),
                new String[]{"key-0,value-0", "key-1,value-1", "key-2,value-2"}
        );
        expectedBlobsAndContent.put(
                getTimestampBlobName(1, 0),
                new String[]{"key-3,value-3"}
        );
        expectedBlobsAndContent.put(
                getTimestampBlobName(3, 0),
                new String[]{"key-4,value-4"}
        );

        final List<String> expectedBlobsNames =
                expectedBlobsAndContent.keySet().stream().sorted().collect(Collectors.toList());
        assertIterableEquals(expectedBlobsNames, testBucketAccessor.getBlobNames(gcsPrefix));

        for (final String expectedBlobName : expectedBlobsNames) {
            final List<String> blobContent = testBucketAccessor
                    .readAndDecodeLines(expectedBlobName, compression, 0, 1)
                    .stream()
                    .map(fields -> String.join(",", fields).trim())
                    .collect(Collectors.toList());

            assertThat(blobContent, containsInAnyOrder(expectedBlobsAndContent.get(expectedBlobName)));
        }
    }

    private String getTimestampBlobName(final int partition, final int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format(
                "%s%s-%d-%d-%s-%s-%s",
                gcsPrefix,
                TEST_TOPIC_0,
                partition,
                startOffset,
                time.format(DateTimeFormatter.ofPattern("yyyy")),
                time.format(DateTimeFormatter.ofPattern("MM")),
                time.format(DateTimeFormatter.ofPattern("dd"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void oneFilePerRecordWithPlainValues(final String compression)
            throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 0, "key-0", "value-0"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 0, "key-1", "value-1"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 0, "key-2", "value-2"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 1, "key-3", "value-3"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC_0, 3, "key-4", "value-4"));

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final Map<String, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getBlobName(0, 0, compression), "value-0");
        expectedBlobsAndContent.put(getBlobName(0, 1, compression), "value-1");
        expectedBlobsAndContent.put(getBlobName(0, 2, compression), "value-2");
        expectedBlobsAndContent.put(getBlobName(1, 0, compression), "value-3");
        expectedBlobsAndContent.put(getBlobName(3, 0, compression), "value-4");
        final List<String> expectedBlobsNames =
                expectedBlobsAndContent.keySet().stream().sorted().collect(Collectors.toList());
        assertIterableEquals(expectedBlobsNames, testBucketAccessor.getBlobNames(gcsPrefix));

        for (final String blobName : expectedBlobsAndContent.keySet()) {
            assertEquals(
                    expectedBlobsAndContent.get(blobName),
                    testBucketAccessor.readStringContent(blobName, compression)
            );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void groupByKey(final String compression) throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final CompressionType compressionType = CompressionType.forName(compression);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("file.name.template", "{{key}}" + compressionType.extension());
        connectRunner.createConnector(connectorConfig);

        final Map<TopicPartition, List<String>> keysPerTopicPartition = new HashMap<>();
        keysPerTopicPartition.put(
                new TopicPartition(TEST_TOPIC_0, 0), Arrays.asList("key-0", "key-1", "key-2", "key-3"));
        keysPerTopicPartition.put(new TopicPartition(TEST_TOPIC_0, 1), Arrays.asList("key-4", "key-5", "key-6"));
        keysPerTopicPartition.put(new TopicPartition(TEST_TOPIC_1, 0), Arrays.asList(null, "key-7"));
        keysPerTopicPartition.put(new TopicPartition(TEST_TOPIC_1, 1), Arrays.asList("key-8"));

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<String, String> lastValuePerKey = new HashMap<>();
        final int cntMax = 1000;
        int cnt = 0;
        outer:
        while (true) {
            for (final TopicPartition tp : keysPerTopicPartition.keySet()) {
                for (final String key : keysPerTopicPartition.get(tp)) {
                    final String value = "value-" + cnt;
                    cnt += 1;
                    sendFutures.add(sendMessageAsync(tp.topic(), tp.partition(), key, value));
                    lastValuePerKey.put(key, value);
                    if (cnt >= cntMax) {
                        break outer;
                    }
                }
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = keysPerTopicPartition.values().stream()
                .flatMap(keys -> keys.stream().map(k -> getBlobName(k, compression)))
                .collect(Collectors.toList());
        assertThat(testBucketAccessor.getBlobNames(gcsPrefix), containsInAnyOrder(expectedBlobs.toArray()));

        for (final String blobName : expectedBlobs) {
            final String blobContent = testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1).stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.joining());
            final String keyInBlobName = blobName.replace(gcsPrefix, "")
                    .replace(compressionType.extension(), "");
            final String value;
            final String expectedBlobContent;
            if (keyInBlobName.equals("null")) {
                value = lastValuePerKey.get(null);
                expectedBlobContent = String.format("%s,%s", "", value);
            } else {
                value = lastValuePerKey.get(keyInBlobName);
                expectedBlobContent = String.format("%s,%s", keyInBlobName, value);
            }
            assertEquals(expectedBlobContent, blobContent);
        }
    }

    private Future<RecordMetadata> sendMessageAsync(final String topicName,
                                                    final int partition,
                                                    final String key,
                                                    final String value) {
        final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(
                topicName, partition,
                key == null ? null : key.getBytes(),
                value == null ? null : value.getBytes());
        return producer.send(msg);
    }

    private Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", GcsSinkConnector.class.getName());
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        if (gcsCredentialsPath != null) {
            config.put("gcs.credentials.path", gcsCredentialsPath);
        }
        if (gcsCredentialsJson != null) {
            config.put("gcs.credentials.json", gcsCredentialsJson);
        }
        config.put("gcs.bucket.name", testBucketName);
        config.put("file.name.prefix", gcsPrefix);
        config.put("topics", TEST_TOPIC_0 + "," + TEST_TOPIC_1);
        return config;
    }

    private String getBlobName(final int partition, final int startOffset, final String compression) {
        final String result = String.format("%s%s-%d-%d", gcsPrefix, TEST_TOPIC_0, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    private String getBlobName(final String key, final String compression) {
        final String result = String.format("%s%s", gcsPrefix, key);
        return result + CompressionType.forName(compression).extension();
    }
}
