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

    private static final String TEST_TOPIC = "test-topic";
    private static final boolean COMPRESS = true;

    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static File testDir;

    private static String testBucketName;

    private static String gcsPrefix;

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private static AdminClient adminClient;
    private static KafkaProducer<byte[], byte[]> producer;
    private static Connect connect;

    private static Storage storage;
    private static BucketAccessor testBucketAccessor;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        testBucketName = System.getProperty("integration-test.gcs.bucket");

        storage = StorageOptions.getDefaultInstance().getService();
        testBucketAccessor = new BucketAccessor(storage, testBucketName);
        testBucketAccessor.ensureWorking();

        gcsPrefix = "aiven-kafka-connect-gcs-test-" +
                ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        testDir = Files.createTempDirectory("aiven-kafka-connect-gcs-test-").toFile();

        final File pluginDir = new File(testDir, "plugins/aiven-kafka-connect-gcs/");
        assert pluginDir.mkdirs();

        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
                distFile.toString(), pluginDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        adminClient = AdminClient.create(adminClientConfig);

        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);

        connect = startConnect(pluginDir);
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
    final void test() throws ExecutionException, InterruptedException, IOException {
        createTopicSync(TEST_TOPIC, 4);

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

        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0),
                getBlobName(1, 0),
                getBlobName(2, 0),
                getBlobName(3, 0));

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

                final String blobName = getBlobName(partition, 0);
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = key + "," + value;
                assertEquals(expectedLine, actualLine);
            }
        }
    }

    private static void createTopicSync(final String name,
                                        final int numPartitions) throws ExecutionException, InterruptedException {
        final NewTopic newTopic = new NewTopic(name, numPartitions, (short) 1);
        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    }

    private static Future<RecordMetadata> sendMessageAsync(final String topicName,
                                                           final int partition,
                                                           final String key,
                                                           final String value) {
        final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(
                topicName, partition,
                key == null ? null : key.getBytes(),
                value == null ? null : value.getBytes());
        return producer.send(msg);
    }

    private static Connect startConnect(final File pluginDir) {
        final Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", KAFKA.getBootstrapServers());

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
        final Herder herder = new StandaloneHerder(worker);

        final RestServer rest = new RestServer(config);

        final Connect connect = new Connect(herder, rest);

        try {
            connect.start();

            final Map<String, String> connectorProps = new HashMap<>();
            connectorProps.put("name", "aiven-gcs-sink-connector");
            connectorProps.put("connector.class", GcsSinkConnector.class.getName());
            connectorProps.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
            connectorProps.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
            connectorProps.put("tasks.max", "1");
            connectorProps.put("gcs.bucket.name", testBucketName);
            connectorProps.put("format.output.fields", "key,value");
            connectorProps.put("topics", TEST_TOPIC);
            connectorProps.put("file.name.prefix", gcsPrefix);
            connectorProps.put("file.compression.type", "gzip");

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
                    connectorProps.get(ConnectorConfig.NAME_CONFIG),
                    connectorProps, false, cb
            );

            final Herder.Created<ConnectorInfo> connectorInfoCreated = cb.get();
            assert connectorInfoCreated.created();
        } catch (final Throwable t) {
            log.error("Stopping after connector error", t);
            connect.stop();
            throw new RuntimeException(t);
        }

        return connect;
    }

    private String getBlobName(final int partition, final int startOffset) {
        String result = String.format("%s%s-%d-%d", gcsPrefix, TEST_TOPIC, partition, startOffset);
        if (COMPRESS) {
            result += ".gz";
        }
        return result;
    }
}
