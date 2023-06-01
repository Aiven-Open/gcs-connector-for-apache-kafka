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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
@Testcontainers
final class AvroIntegrationTest extends AbstractIntegrationTest<String, GenericRecord> {
    private static final String CONNECTOR_NAME = "aiven-gcs-sink-connector-avro";

    @Container
    private final SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer(KAFKA);

    private final Schema avroInputDataSchema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"input_data\"," + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBucketAccessor.clear(gcsPrefix);
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        startConnectRunner(producerProps);
    }

    private void produceRecords(final int recordCountPerPartition) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(avroInputDataSchema);
                value.put("name", "user-" + cnt);
                cnt += 1;

                sendFutures.add(sendMessageAsync(testTopic0, partition, key, value));
            }
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
    }

    @Test
    void avroOutput() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.type", "avro");
        getConnectRunner().createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition);

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0), getAvroBlobName(1, 0),
                getAvroBlobName(2, 0), getAvroBlobName(3, 0));
        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        final Map<String, Schema> gcsOutputAvroSchemas = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final byte[] blobBytes = testBucketAccessor.readBytes(blobName);
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
                final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    final List<GenericRecord> items = new ArrayList<>();
                    reader.forEach(items::add);
                    blobContents.put(blobName, items);
                    gcsOutputAvroSchemas.put(blobName, reader.getSchema());
                }
            }
        }

        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String blobName = getAvroBlobName(partition, 0);
                final Schema gcsOutputAvroSchema = gcsOutputAvroSchemas.get(blobName);
                final GenericData.Record expectedRecord = new GenericData.Record(gcsOutputAvroSchema);
                expectedRecord.put("key", new Utf8("key-" + cnt));
                final GenericData.Record valueRecord = new GenericData.Record(
                        gcsOutputAvroSchema.getField("value").schema());
                valueRecord.put("name", new Utf8("user-" + cnt));
                expectedRecord.put("value", valueRecord);
                cnt += 1;

                final GenericRecord actualRecord = blobContents.get(blobName).get(i);
                assertEquals(expectedRecord, actualRecord);
            }
        }
    }

    private static Stream<Arguments> compressionAndCodecTestParameters() {
        return Stream.of(Arguments.of("bzip2", "none"), Arguments.of("deflate", "none"), Arguments.of("null", "none"),
                Arguments.of("snappy", "gzip"), // single test for codec and compression when both set.
                Arguments.of("zstandard", "none"));
    }

    private byte[] getBlobBytes(final byte[] blobBytes, final String compression) throws IOException {
        switch (CompressionType.forName(compression)) {
            case GZIP :
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(blobBytes)), out);
                return out.toByteArray();
            case NONE :
                return blobBytes;
            default :
                throw new IllegalArgumentException("Unsupported compression in test: " + compression);
        }
    }

    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutputPlainValueWithoutEnvelope(final String avroCodec, final String compression)
            throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.envelope", "false");
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.type", "avro");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("avro.codec", avroCodec);
        getConnectRunner().createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition);

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0, compression),
                getAvroBlobName(1, 0, compression), getAvroBlobName(2, 0, compression),
                getAvroBlobName(3, 0, compression));
        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final byte[] blobBytes = getBlobBytes(testBucketAccessor.readBytes(blobName), compression);
            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
                final List<GenericRecord> items;
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    items = new ArrayList<>();
                    reader.forEach(items::add);
                }
                blobContents.put(blobName, items);
            }
        }

        // Connect will add two extra fields to schema and enrich it with
        // connect.version: 1
        // connect.name: input_data
        final Schema avroInputDataSchemaWithConnectExtra = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"input_data\","
                        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}],"
                        + "\"connect.version\":1,\"connect.name\":\"input_data\"}");
        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String blobName = getAvroBlobName(partition, 0, compression);
                final GenericData.Record expectedRecord = new GenericData.Record(avroInputDataSchemaWithConnectExtra);
                expectedRecord.put("name", new Utf8("user-" + cnt));
                cnt += 1;

                final GenericRecord actualRecord = blobContents.get(blobName).get(i);
                assertEquals(expectedRecord, actualRecord);
            }
        }
    }

    /**
     * When Avro schema changes a new Avro Container File must be produced to GCS. Avro Container File can have only
     * records written with same schema.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    void schemaChanged() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        connectorConfig.put("format.output.envelope", "false");
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("format.output.type", "avro");
        getConnectRunner().createConnector(connectorConfig);

        final Schema evolvedAvroInputDataSchema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"input_data\","
                        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\",\"default\":0}]}");

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final var expectedRecords = new ArrayList<String>();
        // Send only three records, assert three files created.
        for (int i = 0; i < 3; i++) {
            final var key = "key-" + i;
            final GenericRecord value;
            if (i % 2 == 0) { // NOPMD literal
                value = new GenericData.Record(avroInputDataSchema);
                value.put("name", new Utf8("user-" + i));
            } else {
                value = new GenericData.Record(evolvedAvroInputDataSchema);
                value.put("name", new Utf8("user-" + i));
                value.put("age", i);
            }
            expectedRecords.add(value.toString());
            sendFutures.add(sendMessageAsync(testTopic0, 0, key, value));
        }
        getProducer().flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(0, 0), getAvroBlobName(0, 1),
                getAvroBlobName(0, 2));

        awaitAllBlobsWritten(expectedBlobs.size());
        final List<String> blobNames = testBucketAccessor.getBlobNames(gcsPrefix);
        assertIterableEquals(expectedBlobs, blobNames);
        assertEquals(3, blobNames.size());

        final var blobContents = new ArrayList<String>();
        for (final String blobName : expectedBlobs) {
            final byte[] blobBytes = testBucketAccessor.readBytes(blobName);
            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) {
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    reader.forEach(record -> blobContents.add(record.toString()));
                }
            }
        }
        assertIterableEquals(expectedRecords.stream().sorted().collect(Collectors.toList()),
                blobContents.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void jsonlOutput() throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = basicConnectorConfig();
        final String compression = "none";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.type", "jsonl");
        getConnectRunner().createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition);

        final List<String> expectedBlobs = Arrays.asList(getBlobName(0, 0, compression), getBlobName(1, 0, compression),
                getBlobName(2, 0, compression), getBlobName(3, 0, compression));

        awaitAllBlobsWritten(expectedBlobs.size());
        assertIterableEquals(expectedBlobs, testBucketAccessor.getBlobNames(gcsPrefix));

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = new ArrayList<>(testBucketAccessor.readLines(blobName, compression));
            blobContents.put(blobName, items);
        }

        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "{" + "\"name\":\"user-" + cnt + "\"}";
                cnt += 1;

                final String blobName = getBlobName(partition, 0, "none");
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";
                assertEquals(expectedLine, actualLine);
            }
        }
    }

    private Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", GcsSinkConnector.class.getName());
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        if (gcsCredentialsPath != null) {
            config.put("gcs.credentials.path", gcsCredentialsPath);
        }
        if (gcsCredentialsJson != null) {
            config.put("gcs.credentials.json", gcsCredentialsJson);
        }
        if (useFakeGCS()) {
            config.put("gcs.endpoint", gcsEndpoint);
        }
        config.put("gcs.bucket.name", testBucketName);
        config.put("file.name.prefix", gcsPrefix);
        config.put("topics", testTopic0 + "," + testTopic1);
        return config;
    }

    protected String getAvroBlobName(final int partition, final int startOffset, final String compression) {
        return super.getBaseBlobName(partition, startOffset) + ".avro"
                + CompressionType.forName(compression).extension();
    }

    protected String getAvroBlobName(final int partition, final int startOffset) {
        return super.getBaseBlobName(partition, startOffset) + ".avro";
    }
}
