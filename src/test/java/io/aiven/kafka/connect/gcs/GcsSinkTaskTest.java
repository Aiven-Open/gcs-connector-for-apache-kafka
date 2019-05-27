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
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.Lists;
import io.aiven.kafka.connect.gcs.config.GcsSinkConfig;
import io.aiven.kafka.connect.gcs.testutils.BlobAccessor;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

final class GcsSinkTaskTest {

    private final static String TEST_BUCKET = "test-bucket";

    private static Storage storage;
    private static BucketAccessor testBucketAccessor;

    private static Map<String, String> properties;

    private final List<SinkRecord> basicRecords = Arrays.asList(
            createRecord("topic0", 0, "key0", "value0", 10, 1000),
            createRecord("topic0", 1, "key1", "value1", 20, 1001),
            createRecord("topic1", 0, "key2", "value2", 30, 1002),
            createRecord("topic1", 1, "key3", "value3", 40, 1003),
            createRecord("topic0", 2, "key4", "value4", 50, 1004),

            createRecord("topic0", 0, "key5", "value5", 11, 1005),
            createRecord("topic0", 1, "key6", "value6", 21, 1006),
            createRecord("topic1", 0, "key7", "value7", 31, 1007),
            createRecord("topic1", 1, "key8", "value8", 41, 1008),
            createRecord("topic0", 2, "key9", "value9", 51, 1009)
    );

    @BeforeEach
    final void setUp() {
        storage = LocalStorageHelper.getOptions().getService();
        testBucketAccessor = new BucketAccessor(storage, TEST_BUCKET);

        properties = new HashMap<>();
        properties.put(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, TEST_BUCKET);
    }

    @Test
    final void version() {
        final GcsSinkTask task = new GcsSinkTask(properties, storage);
        assertEquals("test-version", task.version());
    }

    @Test
    final void basic() {
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        assertAll(
                () -> assertIterableEquals(
                        Lists.newArrayList("topic0-0-10", "topic0-1-20", "topic0-2-50", "topic1-0-30", "topic1-1-40"),
                        testBucketAccessor.getBlobNames()
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value0"), Arrays.asList("value5")),
                        readSplittedAndDecodedLinesFromBlob("topic0-0-10", false, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value1"), Arrays.asList("value6")),
                        readSplittedAndDecodedLinesFromBlob("topic0-1-20", false, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value4"), Arrays.asList("value9")),
                        readSplittedAndDecodedLinesFromBlob("topic0-2-50", false, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value2"), Arrays.asList("value7")),
                        readSplittedAndDecodedLinesFromBlob("topic1-0-30", false, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value3"), Arrays.asList("value8")),
                        readSplittedAndDecodedLinesFromBlob("topic1-1-40", false, 0)
                )
        );
    }

    @Test
    final void basicValuesPlain() {
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        assertAll(
                () -> assertIterableEquals(
                        Lists.newArrayList("topic0-0-10", "topic0-1-20", "topic0-2-50", "topic1-0-30", "topic1-1-40"),
                        testBucketAccessor.getBlobNames()
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value0"), Arrays.asList("value5")),
                        readSplittedAndDecodedLinesFromBlob("topic0-0-10", false)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value1"), Arrays.asList("value6")),
                        readSplittedAndDecodedLinesFromBlob("topic0-1-20", false)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value4"), Arrays.asList("value9")),
                        readSplittedAndDecodedLinesFromBlob("topic0-2-50", false)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value2"), Arrays.asList("value7")),
                        readSplittedAndDecodedLinesFromBlob("topic1-0-30", false)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value3"), Arrays.asList("value8")),
                        readSplittedAndDecodedLinesFromBlob("topic1-1-40", false)
                )
        );
    }

    @Test
    final void compression() {
        properties.put(GcsSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, "gzip");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        assertAll(
                () -> assertIterableEquals(
                        Lists.newArrayList("topic0-0-10.gz", "topic0-1-20.gz", "topic0-2-50.gz", "topic1-0-30.gz", "topic1-1-40.gz"),
                        testBucketAccessor.getBlobNames()
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value0"), Arrays.asList("value5")),
                        readSplittedAndDecodedLinesFromBlob("topic0-0-10.gz", true, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value1"), Arrays.asList("value6")),
                        readSplittedAndDecodedLinesFromBlob("topic0-1-20.gz", true, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value4"), Arrays.asList("value9")),
                        readSplittedAndDecodedLinesFromBlob("topic0-2-50.gz", true, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value2"), Arrays.asList("value7")),
                        readSplittedAndDecodedLinesFromBlob("topic1-0-30.gz", true, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value3"), Arrays.asList("value8")),
                        readSplittedAndDecodedLinesFromBlob("topic1-1-40.gz", true, 0)
                )
        );
    }

    @Test
    final void allFields() {
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,timestamp,offset");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        assertAll(
                () -> assertIterableEquals(
                        Lists.newArrayList("topic0-0-10", "topic0-1-20", "topic0-2-50", "topic1-0-30", "topic1-1-40"),
                        testBucketAccessor.getBlobNames()
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("key0", "value0", "1000", "10"), Arrays.asList("key5", "value5", "1005", "11")),
                        readSplittedAndDecodedLinesFromBlob("topic0-0-10", false, 0, 1)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("key1", "value1", "1001", "20"), Arrays.asList("key6", "value6", "1006", "21")),
                        readSplittedAndDecodedLinesFromBlob("topic0-1-20", false, 0, 1)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("key4", "value4", "1004", "50"), Arrays.asList("key9", "value9", "1009", "51")),
                        readSplittedAndDecodedLinesFromBlob("topic0-2-50", false, 0, 1)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("key2", "value2", "1002", "30"), Arrays.asList("key7", "value7", "1007", "31")),
                        readSplittedAndDecodedLinesFromBlob("topic1-0-30", false, 0, 1)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("key3", "value3", "1003", "40"), Arrays.asList("key8", "value8", "1008", "41")),
                        readSplittedAndDecodedLinesFromBlob("topic1-1-40", false, 0, 1)
                )
        );
    }

    @Test
    final void nullKeyValueAndTimestamp() {
        properties.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,timestamp,offset");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final List<SinkRecord> records = Arrays.asList(
                createNullRecord("topic0", 0, 10),
                createNullRecord("topic0", 0, 11),
                createNullRecord("topic0", 0, 12)
        );
        task.put(records);
        task.flush(null);

        assertAll(
                () -> assertIterableEquals(
                        Lists.newArrayList("topic0-0-10"),
                        testBucketAccessor.getBlobNames()
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(",,,10", ",,,11", ",,,12"),
                        readRawLinesFromBlob("topic0-0-10", false)
                )
        );
    }

    @Test
    final void multipleFlush() {
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(Arrays.asList(
                createRecord("topic0", 0, "key0", "value0", 100, 1000)));
        task.put(Arrays.asList(
                createRecord("topic0", 0, "key1", "value1", 101, 1001)));
        task.flush(null);
        task.put(Arrays.asList(
                createRecord("topic0", 0, "key2", "value2", 102, 1002)));
        task.put(Arrays.asList(
                createRecord("topic0", 0, "key3", "value3", 103, 1003)));
        task.flush(null);
        task.put(Arrays.asList(
                createRecord("topic0", 0, "key4", "value4", 104, 1004)));
        task.put(Arrays.asList(
                createRecord("topic0", 0, "key5", "value5", 105, 1005)));
        task.flush(null);

        assertAll(
                () -> assertIterableEquals(
                        Lists.newArrayList("topic0-0-100", "topic0-0-102", "topic0-0-104"),
                        testBucketAccessor.getBlobNames()
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value0"), Arrays.asList("value1")),
                        readSplittedAndDecodedLinesFromBlob("topic0-0-100", false, 0)
                ),
                () -> assertIterableEquals(
                        Lists.newArrayList(Arrays.asList("value2"), Arrays.asList("value3")),
                        readSplittedAndDecodedLinesFromBlob("topic0-0-102", false, 0)
                )
        );
    }

    @Test
    final void maxRecordPerFile() throws IOException {
        properties.put(GcsSinkConfig.FILE_MAX_RECORDS, "1");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        final int recordNum = 100;

        for (int i = 0; i < recordNum; i++) {
            final SinkRecord record = createRecord("topic0", 0, "key" + i, "value" + i, i, i);
            task.put(Collections.singletonList(record));
        }
        task.flush(null);

        assertIterableEquals(
                IntStream.range(0, recordNum).mapToObj(i -> "topic0-0-" + i).sorted().collect(Collectors.toList()),
                testBucketAccessor.getBlobNames()
        );
        for (int i = 0; i < recordNum; i++) {
            assertIterableEquals(
                    Collections.singletonList(Collections.singletonList("value" + i)),
                    readSplittedAndDecodedLinesFromBlob("topic0-0-" + i, false, 0)
            );
        }
    }

    @Test
    final void prefix() {
        properties.put(GcsSinkConfig.FILE_NAME_PREFIX_CONFIG, "prefix-");
        final GcsSinkTask task = new GcsSinkTask(properties, storage);

        task.put(basicRecords);
        task.flush(null);

        assertIterableEquals(
                Lists.newArrayList(
                        "prefix-topic0-0-10",
                        "prefix-topic0-1-20",
                        "prefix-topic0-2-50",
                        "prefix-topic1-0-30",
                        "prefix-topic1-1-40"),
                testBucketAccessor.getBlobNames()
        );
    }

    private SinkRecord createRecord(final String topic,
                                    final int partition,
                                    final String key,
                                    final String value,
                                    final int offset,
                                    final long timestamp) {
        return new SinkRecord(
                topic,
                partition,
                Schema.BYTES_SCHEMA,
                key.getBytes(StandardCharsets.UTF_8),
                Schema.BYTES_SCHEMA,
                value.getBytes(StandardCharsets.UTF_8),
                offset,
                timestamp,
                TimestampType.CREATE_TIME);
    }

    private SinkRecord createNullRecord(final String topic,
                                        final int partition,
                                        final int offset) {
        return new SinkRecord(
                topic,
                partition,
                Schema.BYTES_SCHEMA,
                null,
                Schema.BYTES_SCHEMA,
                null,
                offset,
                null,
                TimestampType.NO_TIMESTAMP_TYPE);
    }

    private Collection<String> readRawLinesFromBlob(
            final String blobName,
            final boolean compressed) throws IOException {
        final BlobAccessor blobAccessor = new BlobAccessor(storage, TEST_BUCKET, blobName, compressed);
        return blobAccessor.readLines();
    }

    private Collection<List<String>> readSplittedAndDecodedLinesFromBlob(
            final String blobName,
            final boolean compressed,
            final int... fieldsToDecode) throws IOException {
        final BlobAccessor blobAccessor = new BlobAccessor(storage, TEST_BUCKET, blobName, compressed);
        return blobAccessor.readAndDecodeLines(fieldsToDecode);
    }
}
