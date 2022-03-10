/*
 * Copyright 2021 Aiven Oy
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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Testcontainers
final class ParquetIntegrationTest extends AbstractIntegrationTest {

    private static final String CONNECTOR_NAME = "aiven-gcs-sink-connector-parquet";

    @Container
    private final KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;

    private ConnectRunner connectRunner;
    @TempDir
    Path tmpDir;


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

    @Test
    final void allOutputFields()
            throws ExecutionException, InterruptedException, IOException {
        final var compression = "none";
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
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

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0, compression),
                getBlobName(1, 0, compression),
                getBlobName(2, 0, compression),
                getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);
                assertEquals(key, record.get("key").toString());
                assertEquals(value, record.get("value").toString());
                assertNotNull(record.get("offset"));
                assertNotNull(record.get("timestamp"));
                assertNull(record.get("headers"));
                cnt += 1;
            }
        }
    }

    @Test
    final void allOutputFieldsJsonValueAsString()
            throws ExecutionException, InterruptedException, IOException {
        final var compression = "none";
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "{\"name\": \"name-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                cnt += 1;
                sendFutures.add(sendMessageAsync(TEST_TOPIC_0, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0, compression),
                getBlobName(1, 0, compression),
                getBlobName(2, 0, compression),
                getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final var value = "{\"name\": \"name-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                final String blobName = getBlobName(partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);
                assertEquals(key, record.get("key").toString());
                assertEquals(value, record.get("value").toString());
                assertNotNull(record.get("offset"));
                assertNotNull(record.get("timestamp"));
                assertNull(record.get("headers"));
                cnt += 1;
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"true, {\"value\": {\"name\": \"%s\"}} ", "false, {\"name\": \"%s\"}"})
    final void jsonValue(final String envelopeEnabled, final String expectedOutput)
            throws ExecutionException, InterruptedException, IOException {
        final var compression = "none";
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.envelope", envelopeEnabled);
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectRunner.createConnector(connectorConfig);

        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value =
                        String.format(
                                jsonMessagePattern,
                                jsonMessageSchema, "{" + "\"name\":\"user-" + cnt + "\"}"
                        );
                cnt += 1;

                sendFutures.add(sendMessageAsync(TEST_TOPIC_0, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0, compression),
                getBlobName(1, 0, compression),
                getBlobName(2, 0, compression),
                getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final String blobName = getBlobName(partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);
                final String expectedLine = String.format(expectedOutput, name);
                assertEquals(expectedLine, record.toString());
                cnt += 1;
            }
        }
    }

    @Test
    final void schemaChanged()
            throws ExecutionException, InterruptedException, IOException {
        final var compression = "none";
        final Map<String, String> connectorConfig = basicConnectorConfig(compression);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectRunner.createConnector(connectorConfig);

        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessageNewSchema = "{\"type\":\"struct\",\"fields\":"
                + "[{\"type\":\"string\",\"field\":\"name\"}, "
                + "{\"type\":\"string\",\"field\":\"value\", \"default\": \"foo\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final var expectedRecords = new ArrayList<String>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final String value;
                final String payload;
                if (i < 5) {
                    payload = "{" + "\"name\": \"user-" + cnt + "\"}";
                    value = String.format(jsonMessagePattern, jsonMessageSchema, payload);
                } else {
                    payload = "{" + "\"name\": \"user-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                    value = String.format(jsonMessagePattern, jsonMessageNewSchema, payload);
                }
                expectedRecords.add(String.format("{\"value\": %s}", payload));
                sendFutures.add(sendMessageAsync(TEST_TOPIC_0, partition, key, value));
                cnt += 1;
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0, compression),
                getBlobName(0, 5, compression),
                getBlobName(1, 0, compression),
                getBlobName(1, 5, compression),
                getBlobName(2, 0, compression),
                getBlobName(2, 5, compression),
                getBlobName(3, 0, compression),
                getBlobName(3, 5, compression)
        );

        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final var blobContents = new ArrayList<String>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.addAll(records.stream().map(GenericRecord::toString).collect(Collectors.toList()));
        }
        assertIterableEquals(
                expectedRecords.stream().sorted().collect(Collectors.toList()),
                blobContents.stream().sorted().collect(Collectors.toList())
        );
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

    private Map<String, String> basicConnectorConfig(final String compression) {
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
        config.put("file.compression.type", compression);
        config.put("format.output.type", "parquet");
        return config;
    }

}
