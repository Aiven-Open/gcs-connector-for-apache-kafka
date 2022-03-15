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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;
import io.aiven.kafka.connect.gcs.testutils.Record;
import io.aiven.kafka.connect.gcs.testutils.Utils;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.assertj.core.util.introspection.FieldSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.threeten.bp.Duration;

final class GcsSinkTaskTest {

    private static final String TEST_BUCKET = "test-bucket";

    private static Storage storage;
    private static BucketAccessor testBucketAccessor;

    private static Map<String, String> properties;

    private final Random random = new Random();

    private final List<SinkRecord> basicRecords = Arrays.asList(createRecord("topic0", 0, "key0", "value0", 10, 1000),
            createRecord("topic0", 1, "key1", "value1", 20, 1001),
            createRecord("topic1", 0, "key2", "value2", 30, 1002),
            createRecord("topic1", 1, "key3", "value3", 40, 1003),
            createRecord("topic0", 2, "key4", "value4", 50, 1004),

            createRecord("topic0", 0, "key5", "value5", 11, 1005),
            createRecord("topic0", 1, "key6", "value6", 21, 1006),
            createRecord("topic1", 0, "key7", "value7", 31, 1007),
            createRecord("topic1", 1, "key8", "value8", 41, 1008),
            createRecord("topic0", 2, "key9", "value9", 51, 1009));

    private List<Record> createTestRecords() {
        final List<Record> records = new ArrayList<>();
        records.add(Record.of("topic0", "key0", "value0", 0, 10, 1000, createHeaders()));
        records.add(Record.of("topic0", "key1", "value1", 1, 20, 1001, createHeaders()));
        records.add(Record.of("topic1", "key2", "value2", 0, 30, 1002, createHeaders()));
        records.add(Record.of("topic1", "key3", "value3", 1, 40, 1003, createHeaders()));
        records.add(Record.of("topic0", "key4", "value4", 2, 50, 1004, createHeaders()));

        records.add(Record.of("topic0", "key5", "value5", 0, 11, 1005, createHeaders()));
        records.add(Record.of("topic0", "key6", "value6", 1, 21, 1006, createHeaders()));
        records.add(Record.of("topic1", "key7", "value7", 0, 31, 1007, createHeaders()));
        records.add(Record.of("topic1", "key8", "value8", 1, 41, 1008, createHeaders()));
        records.add(Record.of("topic0", "key9", "value9", 2, 51, 1009, createHeaders()));
        return records;
    }

    private Map<String, Collection<Record>> toBlobNameWithRecordsMap(final String compression,
            final List<Record> records) {
        final CompressionType compressionType = CompressionType.forName(compression);
        final String extension = compressionType.extension();
        final Map<String, Integer> topicPartitionMinimumOffset = new HashMap<>();
        final Map<String, Collection<Record>> blobNameWithRecordsMap = new HashMap<>();
        for (final Record record : records) {
            final String key = record.topic + "-" + record.partition;
            final int offset = record.offset;
            topicPartitionMinimumOffset.putIfAbsent(key, offset);
            if (topicPartitionMinimumOffset.get(key) > offset) {
                topicPartitionMinimumOffset.put(key, offset);
            }
        }
        for (final Record record : records) {
            final int offset = topicPartitionMinimumOffset.get(record.topic + "-" + record.partition);
            final String key = record.topic + "-" + record.partition + "-" + offset + extension;
            blobNameWithRecordsMap.putIfAbsent(key, new ArrayList<>()); // NOPMD
            blobNameWithRecordsMap.get(key).add(record);
        }
        return blobNameWithRecordsMap;
    }

    private List<SinkRecord> toSinkRecords(final List<Record> records) {
        return records.stream().map(Record::toSinkRecord).collect(Collectors.toList());
    }

    @SuppressFBWarnings
    @BeforeEach
    void setUp() {
        storage = LocalStorageHelper.getOptions().getService();
        testBucketAccessor = new BucketAccessor(storage, TEST_BUCKET);

        properties = new HashMap<>();
        properties.put(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, TEST_BUCKET);
    }

    @Test
    void version() {
        final GcsSinkTask task = new GcsSinkTask(properties, storage);
        assertEquals("test-version", task.version());
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void basic(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        final Map<String, Collection<List<String>>> blobNameWithExtensionValuesMap = buildBlobNameValuesMap(
                compression);

        assertEquals(blobNameWithExtensionValuesMap.keySet(), Sets.newHashSet(testBucketAccessor.getBlobNames()));

        blobNameWithExtensionValuesMap.keySet().forEach(blobNameWithExtension -> {
            final Collection<List<String>> expected = blobNameWithExtensionValuesMap.get(blobNameWithExtension);
            final Collection<List<String>> actual = readSplittedAndDecodedLinesFromBlob(blobNameWithExtension,
                    compression, 0);
            assertIterableEquals(expected, actual);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void basicWithHeaders(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,timestamp,offset,headers");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<Record> records = createTestRecords();
        final List<SinkRecord> sinkRecords = toSinkRecords(records);

        task.put(sinkRecords);
        task.flush(null);

        final Map<String, Collection<Record>> blobNameWithRecordsMap = toBlobNameWithRecordsMap(compression, records);

        assertEquals(blobNameWithRecordsMap.keySet(), Sets.newHashSet(testBucketAccessor.getBlobNames()));

        blobNameWithRecordsMap.keySet().forEach(blobNameWithExtension -> {
            final Collection<Record> actual = readRecords(blobNameWithExtension, compression);
            final Collection<Record> expected = blobNameWithRecordsMap.get(blobNameWithExtension);
            assertIterableEquals(expected, actual);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void basicValuesPlain(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        final Map<String, Collection<List<String>>> blobNameWithExtensionValuesMap = buildBlobNameValuesMap(
                compression);

        assertEquals(blobNameWithExtensionValuesMap.keySet(), Sets.newHashSet(testBucketAccessor.getBlobNames()));

        blobNameWithExtensionValuesMap.keySet().forEach(blobNameWithExtension -> {
            final Collection<List<String>> expected = blobNameWithExtensionValuesMap.get(blobNameWithExtension);
            final Collection<List<String>> actual = readSplittedAndDecodedLinesFromBlob(blobNameWithExtension,
                    compression);
            assertIterableEquals(expected, actual);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void compression(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> names = Lists.newArrayList("topic0-0-10", "topic0-1-20", "topic0-2-50", "topic1-0-30",
                "topic1-1-40");
        final List<String> blobNames = names.stream()
                .map(n -> n + compressionType.extension())
                .collect(Collectors.toList());

        assertIterableEquals(blobNames, testBucketAccessor.getBlobNames());
        assertIterableEquals(
                Lists.newArrayList(Collections.singletonList("value0"), Collections.singletonList("value5")),
                readSplittedAndDecodedLinesFromBlob("topic0-0-10" + compressionType.extension(), compression, 0));
        assertIterableEquals(
                Lists.newArrayList(Collections.singletonList("value1"), Collections.singletonList("value6")),
                readSplittedAndDecodedLinesFromBlob("topic0-1-20" + compressionType.extension(), compression, 0));
        assertIterableEquals(
                Lists.newArrayList(Collections.singletonList("value4"), Collections.singletonList("value9")),
                readSplittedAndDecodedLinesFromBlob("topic0-2-50" + compressionType.extension(), compression, 0));
        assertIterableEquals(
                Lists.newArrayList(Collections.singletonList("value2"), Collections.singletonList("value7")),
                readSplittedAndDecodedLinesFromBlob("topic1-0-30" + compressionType.extension(), compression, 0));
        assertIterableEquals(
                Lists.newArrayList(Collections.singletonList("value3"), Collections.singletonList("value8")),
                readSplittedAndDecodedLinesFromBlob("topic1-1-40" + compressionType.extension(), compression, 0));
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void allFields(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,timestamp,offset");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertIterableEquals(
                Lists.newArrayList("topic0-0-10" + compressionType.extension(),
                        "topic0-1-20" + compressionType.extension(), "topic0-2-50" + compressionType.extension(),
                        "topic1-0-30" + compressionType.extension(), "topic1-1-40" + compressionType.extension()),
                testBucketAccessor.getBlobNames());
        assertIterableEquals(
                Lists.newArrayList(Arrays.asList("key0", "value0", "1000", "10"),
                        Arrays.asList("key5", "value5", "1005", "11")),
                readSplittedAndDecodedLinesFromBlob("topic0-0-10" + compressionType.extension(), compression, 0, 1));
        assertIterableEquals(
                Lists.newArrayList(Arrays.asList("key1", "value1", "1001", "20"),
                        Arrays.asList("key6", "value6", "1006", "21")),
                readSplittedAndDecodedLinesFromBlob("topic0-1-20" + compressionType.extension(), compression, 0, 1));
        assertIterableEquals(
                Lists.newArrayList(Arrays.asList("key4", "value4", "1004", "50"),
                        Arrays.asList("key9", "value9", "1009", "51")),
                readSplittedAndDecodedLinesFromBlob("topic0-2-50" + compressionType.extension(), compression, 0, 1));
        assertIterableEquals(
                Lists.newArrayList(Arrays.asList("key2", "value2", "1002", "30"),
                        Arrays.asList("key7", "value7", "1007", "31")),
                readSplittedAndDecodedLinesFromBlob("topic1-0-30" + compressionType.extension(), compression, 0, 1));
        assertIterableEquals(
                Lists.newArrayList(Arrays.asList("key3", "value3", "1003", "40"),
                        Arrays.asList("key8", "value8", "1008", "41")),
                readSplittedAndDecodedLinesFromBlob("topic1-1-40" + compressionType.extension(), compression, 0, 1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void nullKeyValueAndTimestamp(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,timestamp,offset");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(createNullRecord("topic0", 0, 10),
                createNullRecord("topic0", 0, 11), createNullRecord("topic0", 0, 12));
        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertIterableEquals(Lists.newArrayList("topic0-0-10" + compressionType.extension()),
                testBucketAccessor.getBlobNames());
        assertIterableEquals(Lists.newArrayList(",,,10", ",,,11", ",,,12"),
                readRawLinesFromBlob("topic0-0-10" + compressionType.extension(), compression));
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void multipleFlush(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(List.of(createRecord("topic0", 0, "key0", "value0", 100, 1000)));
        task.put(List.of(createRecord("topic0", 0, "key1", "value1", 101, 1001)));
        task.flush(null);
        task.put(List.of(createRecord("topic0", 0, "key2", "value2", 102, 1002)));
        task.put(List.of(createRecord("topic0", 0, "key3", "value3", 103, 1003)));
        task.flush(null);
        task.put(List.of(createRecord("topic0", 0, "key4", "value4", 104, 1004)));
        task.put(List.of(createRecord("topic0", 0, "key5", "value5", 105, 1005)));
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertIterableEquals(
                Lists.newArrayList("topic0-0-100" + compressionType.extension(),
                        "topic0-0-102" + compressionType.extension(), "topic0-0-104" + compressionType.extension()),
                testBucketAccessor.getBlobNames());
        assertIterableEquals(Lists.newArrayList(Arrays.asList("value0"), Arrays.asList("value1")),
                readSplittedAndDecodedLinesFromBlob("topic0-0-100" + compressionType.extension(), compression, 0));
        assertIterableEquals(Lists.newArrayList(Arrays.asList("value2"), Arrays.asList("value3")),
                readSplittedAndDecodedLinesFromBlob("topic0-0-102" + compressionType.extension(), compression, 0));
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void maxRecordPerFile(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FILE_MAX_RECORDS, "1");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final int recordNum = 100;

        for (int i = 0; i < recordNum; i++) {
            final SinkRecord record = createRecord("topic0", 0, "key" + i, "value" + i, i, i);
            task.put(Collections.singletonList(record));
        }
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertIterableEquals(IntStream.range(0, recordNum)
                .mapToObj(i -> "topic0-0-" + i + compressionType.extension())
                .sorted()
                .collect(Collectors.toList()), testBucketAccessor.getBlobNames());
        for (int i = 0; i < recordNum; i++) {
            assertIterableEquals(Collections.singletonList(Collections.singletonList("value" + i)),
                    readSplittedAndDecodedLinesFromBlob("topic0-0-" + i + compressionType.extension(), compression, 0));
        }
    }

    @Test
    void setupDefaultRetryPolicy() {
        final var mockedContext = mock(SinkTaskContext.class);
        final var task = new GcsSinkTask();
        task.initialize(mockedContext);

        final var props = Map.of("gcs.bucket.name", "the_bucket", "gcs.credentials.path",
                Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json").getPath());

        task.start(props);

        final var storage = FieldSupport.EXTRACTION.fieldValue("storage", Storage.class, task);
        final var retrySettings = storage.getOptions().getRetrySettings();

        verify(mockedContext, never()).timeout(anyLong());

        assertThat(retrySettings.isJittered()).isTrue();
        assertThat(retrySettings.getInitialRetryDelay())
                .isEqualTo(Duration.ofMillis(GcsSinkConfig.GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT));
        assertThat(retrySettings.getMaxRetryDelay())
                .isEqualTo(Duration.ofMillis(GcsSinkConfig.GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT));
        assertThat(retrySettings.getRetryDelayMultiplier())
                .isEqualTo(GcsSinkConfig.GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT);
        assertThat(retrySettings.getTotalTimeout())
                .isEqualTo(Duration.ofMillis(GcsSinkConfig.GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_DEFAULT));
        assertThat(retrySettings.getMaxAttempts()).isEqualTo(GcsSinkConfig.GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT);
    }

    @Test
    void setupCustomRetryPolicy() {
        final var mockedContext = mock(SinkTaskContext.class);
        final var kafkaBackoffMsCaptor = ArgumentCaptor.forClass(Long.class);
        final var task = new GcsSinkTask();
        task.initialize(mockedContext);

        final var props = Map.of("gcs.bucket.name", "the_bucket", "gcs.credentials.path",
                Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json").getPath(),
                "kafka.retry.backoff.ms", "1", "gcs.retry.backoff.initial.delay.ms", "2",
                "gcs.retry.backoff.max.delay.ms", "3", "gcs.retry.backoff.delay.multiplier", "4",
                "gcs.retry.backoff.total.timeout.ms", "5", "gcs.retry.backoff.max.attempts", "6");

        task.start(props);

        final var storage = FieldSupport.EXTRACTION.fieldValue("storage", Storage.class, task);
        final var retrySettings = storage.getOptions().getRetrySettings();

        verify(mockedContext).timeout(kafkaBackoffMsCaptor.capture());

        assertThat(retrySettings.isJittered()).isTrue();
        assertThat(kafkaBackoffMsCaptor.getValue()).isEqualTo(1L);
        assertThat(retrySettings.getInitialRetryDelay()).isEqualTo(Duration.ofMillis(2L));
        assertThat(retrySettings.getMaxRetryDelay()).isEqualTo(Duration.ofMillis(3L));
        assertThat(retrySettings.getRetryDelayMultiplier()).isEqualTo(4.0D);
        assertThat(retrySettings.getTotalTimeout()).isEqualTo(Duration.ofMillis(5L));
        assertThat(retrySettings.getMaxAttempts()).isEqualTo(6);
    }

    @Test
    void prefix() {
        properties.put(GcsSinkConfig.FILE_NAME_PREFIX_CONFIG, "prefix-");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        assertIterableEquals(Lists.newArrayList("prefix-topic0-0-10", "prefix-topic0-1-20", "prefix-topic0-2-50",
                "prefix-topic1-0-30", "prefix-topic1-1-40"), testBucketAccessor.getBlobNames());
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void groupByKey(final String compression) {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put("file.name.template", "{{key}}");

        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(createRecordStringKey("topic0", 0, "key0", "value0", 10, 1000),
                createRecordStringKey("topic0", 1, "key1", "value1", 20, 1001),
                createRecordStringKey("topic1", 0, "key2", "value2", 30, 1002),

                createRecordStringKey("topic0", 0, "key1", "value3", 11, 1005),
                createRecordStringKey("topic0", 1, "key1", "value4", 21, 1006),
                createRecordStringKey("topic1", 0, null, "value5", 31, 1007),

                createRecordStringKey("topic0", 0, "key0", "value6", 12, 1009),
                createRecordStringKey("topic0", 1, "key1", "value7", 22, 1010),
                createRecordStringKey("topic1", 0, "key1", "value8", 32, 1011));

        task.put(records);
        task.flush(null);

        assertIterableEquals(Lists.newArrayList("key0", "key1", "key2", "null"), testBucketAccessor.getBlobNames());

        assertIterableEquals(Arrays.asList(Arrays.asList("key0", "value6")),
                readSplittedAndDecodedLinesFromBlob("key0", compression, 0, 1));
        assertIterableEquals(Arrays.asList(Arrays.asList("key1", "value8")),
                readSplittedAndDecodedLinesFromBlob("key1", compression, 0, 1));
        assertIterableEquals(Arrays.asList(Arrays.asList("key2", "value2")),
                readSplittedAndDecodedLinesFromBlob("key2", compression, 0, 1));
        assertIterableEquals(Arrays.asList(Arrays.asList("", "value5")), // null is written as an empty string to files
                readSplittedAndDecodedLinesFromBlob("null", compression, 0, 1));
    }

    @Test
    void failedForStringValuesByDefault() {
        final String compression = "none";
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");

        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createRecordWithStringValueSchema("topic0", 0, "key0", "value0", 10, 1000),
                createRecordWithStringValueSchema("topic0", 1, "key1", "value1", 20, 1001),
                createRecordWithStringValueSchema("topic1", 0, "key2", "value2", 30, 1002)

        );

        task.put(records);

        final Throwable throwable = assertThrows(ConnectException.class, () -> task.flush(null));
        assertEquals("org.apache.kafka.connect.errors.DataException: "
                + "Record value schema type must be BYTES, STRING given", throwable.getMessage());
    }

    @Test
    void supportStringValuesForJsonL() {
        final String compression = "none";
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "jsonl");

        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createRecordWithStringValueSchema("topic0", 0, "key0", "value0", 10, 1000),
                createRecordWithStringValueSchema("topic0", 1, "key1", "value1", 20, 1001),
                createRecordWithStringValueSchema("topic1", 0, "key2", "value2", 30, 1002));

        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertIterableEquals(
                Lists.newArrayList("topic0-0-10" + compressionType.extension(),
                        "topic0-1-20" + compressionType.extension(), "topic1-0-30" + compressionType.extension()),
                testBucketAccessor.getBlobNames());

        assertIterableEquals(Arrays.asList("{\"value\":\"value0\",\"key\":\"key0\"}"),
                readRawLinesFromBlob("topic0-0-10", compression));
        assertIterableEquals(Arrays.asList("{\"value\":\"value1\",\"key\":\"key1\"}"),
                readRawLinesFromBlob("topic0-1-20", compression));
        assertIterableEquals(Arrays.asList("{\"value\":\"value2\",\"key\":\"key2\"}"),
                readRawLinesFromBlob("topic1-0-30", compression));
    }

    @Test
    void failedForStructValuesByDefault() {
        final String compression = "none";
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002)

        );

        task.put(records);

        final Throwable throwable = assertThrows(ConnectException.class, () -> task.flush(null));
        assertEquals("org.apache.kafka.connect.errors.DataException: "
                + "Record value schema type must be BYTES, STRUCT given", throwable.getMessage());
    }

    @Test
    void supportStructValuesForJsonL() {
        final String compression = "none";
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "jsonl");

        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002)

        );

        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertIterableEquals(
                Lists.newArrayList("topic0-0-10" + compressionType.extension(),
                        "topic0-1-20" + compressionType.extension(), "topic1-0-30" + compressionType.extension()),
                testBucketAccessor.getBlobNames());

        assertIterableEquals(Arrays.asList("{\"value\":{\"name\":\"name0\"},\"key\":\"key0\"}"),
                readRawLinesFromBlob("topic0-0-10", compression));
        assertIterableEquals(Arrays.asList("{\"value\":{\"name\":\"name1\"},\"key\":\"key1\"}"),
                readRawLinesFromBlob("topic0-1-20", compression));
        assertIterableEquals(Arrays.asList("{\"value\":{\"name\":\"name2\"},\"key\":\"key2\"}"),
                readRawLinesFromBlob("topic1-0-30", compression));
    }

    @Test
    void supportUnwrappedJsonEnvelopeForStructAndJsonL() {
        final String compression = "none";
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "value");
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_ENVELOPE_CONFIG, "false");
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "jsonl");

        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002));

        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertThat(testBucketAccessor.getBlobNames()).containsExactly("topic0-0-10" + compressionType.extension(),
                "topic0-1-20" + compressionType.extension(), "topic1-0-30" + compressionType.extension());

        assertThat(readRawLinesFromBlob("topic0-0-10", compression)).containsExactly("{\"name\":\"name0\"}");
        assertThat(readRawLinesFromBlob("topic0-1-20", compression)).containsExactly("{\"name\":\"name1\"}");
        assertThat(readRawLinesFromBlob("topic1-0-30", compression)).containsExactly("{\"name\":\"name2\"}");
    }

    @Test
    void supportStructValuesForClassicJson() {
        final String compression = "none";
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "json");

        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002)

        );

        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertIterableEquals(
                Lists.newArrayList("topic0-0-10" + compressionType.extension(),
                        "topic0-1-20" + compressionType.extension(), "topic1-0-30" + compressionType.extension()),
                testBucketAccessor.getBlobNames());

        assertIterableEquals(Arrays.asList("[", "{\"value\":{\"name\":\"name0\"},\"key\":\"key0\"}", "]"),
                readRawLinesFromBlob("topic0-0-10", compression));
        assertIterableEquals(Arrays.asList("[", "{\"value\":{\"name\":\"name1\"},\"key\":\"key1\"}", "]"),
                readRawLinesFromBlob("topic0-1-20", compression));
        assertIterableEquals(Arrays.asList("[", "{\"value\":{\"name\":\"name2\"},\"key\":\"key2\"}", "]"),
                readRawLinesFromBlob("topic1-0-30", compression));
    }

    @Test
    void supportUnwrappedJsonEnvelopeForStructAndClassicJson() {
        final String compression = "none";
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "value");
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_ENVELOPE_CONFIG, "false");
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "json");

        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002));

        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        assertThat(testBucketAccessor.getBlobNames()).containsExactly("topic0-0-10" + compressionType.extension(),
                "topic0-1-20" + compressionType.extension(), "topic1-0-30" + compressionType.extension());

        assertThat(readRawLinesFromBlob("topic0-0-10", compression)).containsExactly("[", "{\"name\":\"name0\"}", "]");
        assertThat(readRawLinesFromBlob("topic0-1-20", compression)).containsExactly("[", "{\"name\":\"name1\"}", "]");
        assertThat(readRawLinesFromBlob("topic1-0-30", compression)).containsExactly("[", "{\"name\":\"name2\"}", "]");
    }

    private SinkRecord createRecordWithStructValueSchema(final String topic, final int partition, final String key,
            final String name, final int offset, final long timestamp) {
        final Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA);
        final Struct struct = new Struct(schema).put("name", name);
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, schema, struct, offset, timestamp,
                TimestampType.CREATE_TIME);
    }

    private SinkRecord createRecordWithStringValueSchema(final String topic, final int partition, final String key,
            final String value, final int offset, final long timestamp) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, value, offset,
                timestamp, TimestampType.CREATE_TIME);
    }

    private SinkRecord createRecord(final String topic, final int partition, final String key, final String value,
            final int offset, final long timestamp) {
        return new SinkRecord(topic, partition, Schema.BYTES_SCHEMA, key.getBytes(StandardCharsets.UTF_8),
                Schema.BYTES_SCHEMA, value.getBytes(StandardCharsets.UTF_8), offset, timestamp,
                TimestampType.CREATE_TIME);
    }

    private Iterable<Header> createHeaders() {
        final byte[] bytes1 = new byte[8];
        final byte[] bytes2 = new byte[8];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
        final String key1 = Utils.bytesToHex(bytes1);
        final String key2 = Utils.bytesToHex(bytes2);
        final byte[] value1 = new byte[32];
        final byte[] value2 = new byte[32];
        random.nextBytes(value1);
        random.nextBytes(value2);
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addBytes(key1, value1);
        connectHeaders.addBytes(key2, value2);
        return connectHeaders;
    }

    private SinkRecord createRecordStringKey(final String topic, final int partition, final String key,
            final String value, final int offset, final long timestamp) {
        return new SinkRecord(topic, partition, Schema.OPTIONAL_STRING_SCHEMA, key, Schema.BYTES_SCHEMA,
                value.getBytes(StandardCharsets.UTF_8), offset, timestamp, TimestampType.CREATE_TIME);
    }

    private SinkRecord createNullRecord(final String topic, final int partition, final int offset) {
        return new SinkRecord(topic, partition, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, null, offset, null,
                TimestampType.NO_TIMESTAMP_TYPE);
    }

    private List<Record> readRecords(final String blobName, final String compression) {
        return testBucketAccessor.decodeToRecords(blobName, compression);
    }

    private Collection<String> readRawLinesFromBlob(final String blobName, final String compression) {
        return testBucketAccessor.readLines(blobName, compression);
    }

    private Collection<List<String>> readSplittedAndDecodedLinesFromBlob(final String blobName,
            final String compression, final int... fieldsToDecode) {
        return testBucketAccessor.readAndDecodeLines(blobName, compression, fieldsToDecode);
    }

    private Map<String, Collection<List<String>>> buildBlobNameValuesMap(final String compression) {
        final CompressionType compressionType = CompressionType.forName(compression);
        final String extension = compressionType.extension();
        final Map<String, Collection<List<String>>> blobNameValuesMap = new HashMap<>();
        blobNameValuesMap.put("topic0-0-10" + extension, toCollectionOfLists("value0", "value5"));
        blobNameValuesMap.put("topic0-1-20" + extension, toCollectionOfLists("value1", "value6"));
        blobNameValuesMap.put("topic1-0-30" + extension, toCollectionOfLists("value2", "value7"));
        blobNameValuesMap.put("topic1-1-40" + extension, toCollectionOfLists("value3", "value8"));
        blobNameValuesMap.put("topic0-2-50" + extension, toCollectionOfLists("value4", "value9"));
        return blobNameValuesMap;
    }

    /*
     * example Input: "value0", "value5" Output: Collection[List["value0"], List["value5"]]
     */
    private Collection<List<String>> toCollectionOfLists(final String... values) {
        return toCollectionOfLists(Lists.newArrayList(values));
    }

    private Collection<List<String>> toCollectionOfLists(final List<String> values) {
        return values.stream().map(Collections::singletonList).collect(Collectors.toList());
    }
}
