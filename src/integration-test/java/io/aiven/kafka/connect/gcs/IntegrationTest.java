/*
 * Aiven Kafka GCS Connector
 * Copyright (c) 2019 Aiven Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.aiven.kafka.connect.gcs.gcs.GoogleCredentialsBuilder;
import io.aiven.kafka.connect.gcs.testutils.BlobAccessor;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

// TODO refactor test to make it more readable
@Testcontainers
final class IntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);

    private static String gcsCredentialsPath;
    private static String gcsCredentialsJson;

    private static final String CONNECTOR_NAME = "aiven-gcs-sink-connector";
    private static final String TEST_TOPIC = "test-topic";

    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static String testBucketName;

    private static String gcsPrefix;

    private static Storage storage;
    private static BucketAccessor testBucketAccessor;

    private static File pluginDir;

    @Container
    private final KafkaContainer kafka = new KafkaContainer()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;

    private Herder herder;
    private Connect connect;


    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        gcsCredentialsPath = System.getProperty("integration-test.gcs.credentials.path");
        gcsCredentialsJson = System.getProperty("integration-test.gcs.credentials.json");

        testBucketName = System.getProperty("integration-test.gcs.bucket");

        storage = StorageOptions.newBuilder()
                .setCredentials(GoogleCredentialsBuilder.build(gcsCredentialsPath, gcsCredentialsJson))
                .build()
                .getService();
        testBucketAccessor = new BucketAccessor(storage, testBucketName);
        testBucketAccessor.ensureWorking();

        gcsPrefix = "aiven-kafka-connect-gcs-test-" +
                ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

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

        final NewTopic newTopic = new NewTopic(TEST_TOPIC, 4, (short) 1);
        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

        startConnect(pluginDir);
    }

    @AfterEach
    final void tearDown() {
        connect.stop();
        adminClient.close();
        producer.close();

        testBucketAccessor.clear(gcsPrefix);

        connect.awaitStop();
    }

    @Test
    final void basicTest() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", "gzip");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                cnt += 1;

                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0, true),
                getBlobName(1, 0, true),
                getBlobName(2, 0, true),
                getBlobName(3, 0, true));
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames());

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final BlobAccessor blobAccessor = new BlobAccessor(storage, testBucketName, blobName, true);
            blobContents.put(blobName,
                    blobAccessor.readAndDecodeLines(0, 1).stream()
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

                final String blobName = getBlobName(partition, 0, true);
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = key + "," + value;
                assertEquals(expectedLine, actualLine);
            }
        }
    }

    @Test
    final void oneFilePerRecordWithPlainValues() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");
        createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(TEST_TOPIC, 0, "key-0", "value-0"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC, 0, "key-1", "value-1"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC, 0, "key-2", "value-2"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC, 1, "key-3", "value-3"));
        sendFutures.add(sendMessageAsync(TEST_TOPIC, 3, "key-4", "value-4"));

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final Map<String, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getBlobName(0, 0, false), "value-0");
        expectedBlobsAndContent.put(getBlobName(0, 1, false), "value-1");
        expectedBlobsAndContent.put(getBlobName(0, 2, false), "value-2");
        expectedBlobsAndContent.put(getBlobName(1, 0, false), "value-3");
        expectedBlobsAndContent.put(getBlobName(3, 0, false), "value-4");
        final List<String> expectedBlobsNames = expectedBlobsAndContent.keySet().stream().sorted().collect(Collectors.toList());
        assertIterableEquals(expectedBlobsNames, testBucketAccessor.getBlobNames());

        for (final String blobName : expectedBlobsAndContent.keySet()) {
            final BlobAccessor blobAccessor = new BlobAccessor(storage, testBucketName, blobName, false);
            assertEquals(expectedBlobsAndContent.get(blobName), blobAccessor.readStringContent());

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

    private void startConnect(final File pluginDir) {
        final Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", kafka.getBootstrapServers());

        workerProps.put("offset.flush.interval.ms", Integer.toString(OFFSET_FLUSH_INTERVAL_MS));

        workerProps.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "false");

        // Don't need it since we'll memory MemoryOffsetBackingStore.
        workerProps.put("offset.storage.file.filename", "");

        workerProps.put("plugin.path", pluginDir.getPath());

        final Time time = Time.SYSTEM;
        final String workerId = "test-worker";

        final Plugins plugins = new Plugins(workerProps);
        final StandaloneConfig config = new StandaloneConfig(workerProps);

        final Worker worker = new Worker(
                workerId, time, plugins, config, new MemoryOffsetBackingStore());
        herder = new StandaloneHerder(worker);

        final RestServer rest = new RestServer(config);

        connect = new Connect(herder, rest);

        connect.start();
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
        config.put("topics", TEST_TOPIC);
        return config;
    }

    private void createConnector(final Map<String, String> config) throws ExecutionException, InterruptedException {
        final FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(
                new Callback<Herder.Created<ConnectorInfo>>() {
                    @Override
                    public void onCompletion(final Throwable error, final Herder.Created<ConnectorInfo> info) {
                        if (error != null) {
                            log.error("Failed to create job");
                        } else {
                            log.info("Created connector {}", info.result().name());
                        }
                    }
                });
        herder.putConnectorConfig(
                config.get(ConnectorConfig.NAME_CONFIG),
                config, false, cb
        );

        final Herder.Created<ConnectorInfo> connectorInfoCreated = cb.get();
        assert connectorInfoCreated.created();
    }

    private String getBlobName(final int partition, final int startOffset, final boolean compress) {
        String result = String.format("%s%s-%d-%d", gcsPrefix, TEST_TOPIC, partition, startOffset);
        if (compress) {
            result += ".gz";
        }
        return result;
    }
}
