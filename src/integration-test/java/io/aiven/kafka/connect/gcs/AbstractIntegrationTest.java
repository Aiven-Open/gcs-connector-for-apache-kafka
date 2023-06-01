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

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;

import com.github.dockerjava.api.model.Ulimit;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class AbstractIntegrationTest<K, V> {
    protected final String testTopic0;
    protected final String testTopic1;

    private AdminClient adminClient;
    private ConnectRunner connectRunner;
    private KafkaProducer<K, V> producer;

    protected static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    protected static final String DEFAULT_GCS_ENDPOINT = "https://storage.googleapis.com";

    private static final int GCS_PORT = getRandomPort();
    public static final String DEFAULT_TEST_PROJECT_NAME = "TEST_LOCAL";
    public static final String DEFAULT_TEST_BUCKET_NAME = "test";
    protected static String gcsCredentialsPath; // NOPMD mutable static state
    protected static String gcsCredentialsJson; // NOPMD mutable static state

    protected static String testBucketName; // NOPMD mutable static state

    protected static String gcsPrefix; // NOPMD mutable static state

    protected static BucketAccessor testBucketAccessor; // NOPMD mutable static state

    protected static File pluginDir; // NOPMD mutable static state
    protected static String gcsEndpoint; // NOPMD mutable static state

    private static final String FAKE_GCS_SERVER_VERSION = System.getProperty("fake-gcs-server-version", "latest");
    @Container
    private static final GenericContainer<?> FAKE_GCS_CONTAINER = new FixedHostPortGenericContainer(
            String.format("fsouza/fake-gcs-server:%s", FAKE_GCS_SERVER_VERSION))
            .withFixedExposedPort(GCS_PORT, GCS_PORT)
            .withCommand("-port", Integer.toString(GCS_PORT), "-scheme", "http")
            .withReuse(true);
    @Container
    protected static final KafkaContainer KAFKA = new KafkaContainer("5.2.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withNetwork(Network.newNetwork())
            .withExposedPorts(KafkaContainer.KAFKA_PORT, 9092)
            .withCreateContainerCmdModifier(
                    cmd -> cmd.getHostConfig().withUlimits(List.of(new Ulimit("nofile", 30_000L, 30_000L))));

    static int getRandomPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to allocate port for test GCS container", e); // NOPMD throwing raw
                                                                                             // exception
        }
    }

    protected AbstractIntegrationTest() {
        testTopic0 = "test-topic-0-" + UUID.randomUUID();
        testTopic1 = "test-topic-1-" + UUID.randomUUID();
    }

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        gcsCredentialsPath = System.getProperty("integration-test.gcs.credentials.path");
        gcsCredentialsJson = System.getProperty("integration-test.gcs.credentials.json");
        final String bucket = System.getProperty("integration-test.gcs.bucket");
        testBucketName = bucket == null || bucket.isEmpty() ? DEFAULT_TEST_BUCKET_NAME : bucket;
        final Storage storage; // NOPMD
        if (useFakeGCS()) {
            gcsEndpoint = "http://" + FAKE_GCS_CONTAINER.getHost() + ":" + GCS_PORT;
            final StorageOptions storageOps = StorageOptions.newBuilder()
                    .setCredentials(NoCredentials.getInstance())
                    .setHost(gcsEndpoint)
                    .setProjectId(DEFAULT_TEST_PROJECT_NAME)
                    .build();
            storage = storageOps.getService();
            storage.create(BucketInfo.of(testBucketName));
        } else {
            gcsEndpoint = DEFAULT_GCS_ENDPOINT;
            final StorageOptions storageOps = StorageOptions.newBuilder()
                    .setCredentials(GoogleCredentialsBuilder.build(gcsCredentialsPath, gcsCredentialsJson))
                    .build();
            storage = storageOps.getService();
        }

        testBucketAccessor = new BucketAccessor(storage, testBucketName);
        testBucketAccessor.ensureWorking();

        gcsPrefix = "gcs-connector-for-apache-kafka-test-"
                + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final File testDir = Files.createTempDirectory("gcs-connector-for-apache-kafka-test-").toFile();

        pluginDir = new File(testDir, "plugins/gcs-connector-for-apache-kafka/");
        assert pluginDir.mkdirs();

        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile.toString(),
                pluginDir.toString());
        final Process process = Runtime.getRuntime().exec(cmd);
        assert process.waitFor() == 0;
    }

    @AfterEach
    void tearDown() {
        connectRunner.stop();
        adminClient.close();
        producer.close();
        testBucketAccessor.clear(gcsPrefix);
        connectRunner.awaitStop();
    }

    protected static boolean useFakeGCS() {
        return gcsCredentialsPath == null && gcsCredentialsJson == null;
    }

    protected String getBaseBlobName(final int partition, final int startOffset) {
        return String.format("%s%s-%d-%d", gcsPrefix, testTopic0, partition, startOffset);
    }

    protected String getBlobName(final int partition, final int startOffset, final String compression) {
        final String result = getBaseBlobName(partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    protected String getBlobName(final String key, final String compression) {
        final String result = String.format("%s%s", gcsPrefix, key);
        return result + CompressionType.forName(compression).extension();
    }

    protected void awaitAllBlobsWritten(final int expectedBlobCount) {
        await("All expected files stored on GCS").atMost(Duration.ofMillis(OFFSET_FLUSH_INTERVAL_MS * 30))
                .pollInterval(Duration.ofMillis(300))
                .until(() -> testBucketAccessor.getBlobNames(gcsPrefix).size() >= expectedBlobCount);

    }

    protected KafkaProducer<K, V> getProducer() {
        return producer;
    }

    protected Future<RecordMetadata> sendMessageAsync(final String topicName, final int partition, final K key,
            final V value) {
        final ProducerRecord<K, V> msg = new ProducerRecord<>(topicName, partition, key, value);
        return producer.send(msg);
    }

    protected ConnectRunner getConnectRunner() {
        return connectRunner;
    }

    protected void startConnectRunner(final Map<String, Object> testSpecificProducerProperties)
            throws ExecutionException, InterruptedException {
        testBucketAccessor.clear(gcsPrefix);

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        adminClient = AdminClient.create(adminClientConfig);

        final Map<String, Object> producerProps = new HashMap<>(testSpecificProducerProperties);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producer = new KafkaProducer<>(producerProps);

        final NewTopic newTopic0 = new NewTopic(testTopic0, 4, (short) 1);
        final NewTopic newTopic1 = new NewTopic(testTopic1, 4, (short) 1);
        adminClient.createTopics(Arrays.asList(newTopic0, newTopic1)).all().get();

        connectRunner = new ConnectRunner(pluginDir, KAFKA.getBootstrapServers(), OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }
}
