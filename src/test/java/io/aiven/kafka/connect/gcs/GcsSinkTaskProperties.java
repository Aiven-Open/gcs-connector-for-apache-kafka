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
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.engine.properties.arbitraries.randomized.RandomGenerators;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * This is a property-based test for {@link GcsSinkTask} using
 * <a href="https://jqwik.net/docs/current/user-guide.html">jqwik</a>.
 *
 * The idea is to generate random batches of {@link SinkRecord}
 * ({@link GcsSinkTaskProperties#recordBatches()}, put them into a task, and check certain properties
 * of the written files afterwards. Files are written virtually using the in-memory GCS mock.
 */
final class GcsSinkTaskProperties {

    private static final int MAX_TOPICS = 6;
    private static final int MAX_PARTITIONS_PER_TOPIC = 10;
    private static final int MAX_START_OFFSET = 20000;
    private static final int MAX_OFFSET_INCREMENT = 5;
    private static final int MAX_RECORDS_PER_TRY = 10000;
    private static final double PROBABILITY_OF_NEW_RECORD_BATCH = 2.0 / MAX_RECORDS_PER_TRY;
    private static final String TEST_BUCKET = "test-bucket";
    private static final String PREFIX = "test-dir/";
    private static final int FIELD_KEY = 0;
    private static final int FIELD_VALUE = 1;
    private static final int FIELD_OFFSET = 2;

    @Property
    final void unlimited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches) {
        genericTry(recordBatches, null);
    }

    @Property
    final void limited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches,
                       @ForAll @IntRange(min = 1, max = 100) final int maxRecordsPerFile) {
        genericTry(recordBatches, maxRecordsPerFile);
    }

    private void genericTry(final List<List<SinkRecord>> recordBatches, final Integer maxRecordsPerFile) {
        final Storage storage = LocalStorageHelper.getOptions().getService();
        final BucketAccessor testBucketAccessor = new BucketAccessor(storage, TEST_BUCKET, true);

        final Map<String, String> taskProps = new HashMap<>();
        taskProps.put(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, TEST_BUCKET);
        taskProps.put(GcsSinkConfig.FILE_NAME_PREFIX_CONFIG, PREFIX);
        taskProps.put(GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG, "{{topic}}-{{partition}}-{{start_offset}}");
        if (maxRecordsPerFile != null) {
            taskProps.put(GcsSinkConfig.FILE_MAX_RECORDS, Integer.toString(maxRecordsPerFile));
        }
        taskProps.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset");

        final GcsSinkTask task = new GcsSinkTask(taskProps, storage);

        for (final List<SinkRecord> recordBatch : recordBatches) {
            task.put(recordBatch);
            task.flush(null);
        }

        checkExpectedFileNames(recordBatches, maxRecordsPerFile, testBucketAccessor);
        checkFileSizes(testBucketAccessor, maxRecordsPerFile);
        final int expectedRecordCount = recordBatches.stream().mapToInt(List::size).sum();
        checkTotalRecordCountAndNoMultipleWrites(expectedRecordCount, testBucketAccessor);
        checkTopicPartitionPartInFileNames(testBucketAccessor);
        checkOffsetOrderInFiles(testBucketAccessor);
    }

    /**
     * Checks that written files have expected names.
     */
    private void checkExpectedFileNames(final List<List<SinkRecord>> recordBatches,
                                        final Integer maxRecordsPerFile,
                                        final BucketAccessor bucketAccessor) {
        final List<String> expectedFileNames = new ArrayList<>();

        for (final List<SinkRecord> recordBatch : recordBatches) {
            final Map<TopicPartition, List<SinkRecord>> groupedPerTopicPartition = recordBatch.stream()
                    .collect(
                            Collectors.groupingBy(r -> new TopicPartition(r.topic(), r.kafkaPartition()))
                    );

            for (final TopicPartition tp : groupedPerTopicPartition.keySet()) {
                final List<List<SinkRecord>> chunks = Lists.partition(
                        groupedPerTopicPartition.get(tp), effectiveMaxRecordsPerFile(maxRecordsPerFile));
                for (final List<SinkRecord> chunk : chunks) {
                    expectedFileNames.add(createFilename(chunk.get(0)));
                }
            }
        }

        assertThat(bucketAccessor.getBlobNames(), containsInAnyOrder(expectedFileNames.toArray()));
    }

    /**
     * For each written file, checks that it's not empty and is not exceeding the maximum size.
     */
    private void checkFileSizes(final BucketAccessor bucketAccessor, final Integer maxRecordsPerFile) {
        final int effectiveMax = effectiveMaxRecordsPerFile(maxRecordsPerFile);
        for (final String filename : bucketAccessor.getBlobNames()) {
            assertThat(
                    bucketAccessor.readLines(filename, false),
                    hasSize(allOf(greaterThan(0), lessThanOrEqualTo(effectiveMax)))
            );
        }
    }

    /**
     * Checks, that:
     * <ul>
     *     <li>the total number of records written to all files is correct;</li>
     *     <li>each record is written only once.</li>
     * </ul>
     */
    private void checkTotalRecordCountAndNoMultipleWrites(final int expectedCount,
                                                          final BucketAccessor bucketAccessor) {
        final Set<String> seenRecords = new HashSet<>();
        for (final String filename : bucketAccessor.getBlobNames()) {
            for (final String line : bucketAccessor.readLines(filename, false)) {
                // Ensure no multiple writes.
                assertFalse(seenRecords.contains(line));
                seenRecords.add(line);
            }
        }
        assertEquals(expectedCount, seenRecords.size());
    }

    /**
     * For each written file, checks that its filename
     * (the topic and partition part) is correct for each record in it.
     */
    private void checkTopicPartitionPartInFileNames(final BucketAccessor bucketAccessor) {
        for (final String filename : bucketAccessor.getBlobNames()) {
            final String filenameWithoutOffset = cutOffsetPart(filename);

            final List<List<String>> lines = bucketAccessor.readAndDecodeLines(filename, false, FIELD_KEY, FIELD_VALUE);
            final String firstLineTopicAndPartition = lines.get(0).get(FIELD_VALUE);
            final String firstLineOffset = lines.get(0).get(FIELD_OFFSET);
            assertEquals(PREFIX + firstLineTopicAndPartition + "-" + firstLineOffset, filename);

            for (final List<String> line : lines) {
                final String value = line.get(FIELD_VALUE);
                assertEquals(PREFIX + value, filenameWithoutOffset);
            }
        }
    }

    /**
     * Cuts off the offset part from a string like "topic-partition-offset".
     */
    private String cutOffsetPart(final String topicPartitionOffset) {
        return topicPartitionOffset.substring(0, topicPartitionOffset.lastIndexOf('-'));
    }

    /**
     * For each written file, checks that offsets of records are increasing.
     */
    private void checkOffsetOrderInFiles(final BucketAccessor bucketAccessor) {
        for (final String filename : bucketAccessor.getBlobNames()) {
            final List<List<String>> lines = bucketAccessor.readAndDecodeLines(filename, false, FIELD_KEY, FIELD_VALUE);
            final List<Integer> offsets = lines.stream()
                    .map(line -> Integer.parseInt(line.get(FIELD_OFFSET)))
                    .collect(Collectors.toList());
            for (int i = 0; i < offsets.size() - 1; i++) {
                assertTrue(offsets.get(i) < offsets.get(i + 1));
            }
        }
    }

    @Provide
    private Arbitrary<List<List<SinkRecord>>> recordBatches() {
        final RandomGenerator<String> keyGenerator = RandomGenerators.samples(
                new String[]{"key0", "key1", "key2", "key3", null});

        // TODO make generated lists shrinkable
        return Arbitraries.randomValue(random -> {
            final List<TopicPartition> topicPartitions = new ArrayList<>();
            final Map<TopicPartition, SinkRecordBuilder> recordBuilders = new HashMap<>();

            // Randomly pick the number of topics.
            final int topicCount = random.nextInt(MAX_TOPICS - 1) + 1;
            for (int topicIdx = 0; topicIdx < topicCount; topicIdx++) {
                // Randomly pick the number of partitions for a particular topic.
                final int partitionCount = random.nextInt(MAX_PARTITIONS_PER_TOPIC - 1) + 1;
                for (int partitionIdx = 0; partitionIdx < partitionCount; partitionIdx++) {
                    final TopicPartition tp = new TopicPartition("topic" + topicIdx, partitionIdx);
                    topicPartitions.add(tp);
                    recordBuilders.put(tp, new SinkRecordBuilder(random, keyGenerator, tp.topic(), tp.partition()));
                }
            }

            // Randomly pick the number of records to generate in this try.
            final int numberOrRecords = random.nextInt(MAX_RECORDS_PER_TRY);
            final List<List<SinkRecord>> result = new ArrayList<>(numberOrRecords);
            result.add(new ArrayList<>());
            for (int i = 0; i < numberOrRecords; i++) {
                if (random.nextDouble() < PROBABILITY_OF_NEW_RECORD_BATCH) {
                    result.add(new ArrayList<>());
                }
                final TopicPartition tp = topicPartitions.get(random.nextInt(topicPartitions.size()));
                final List<SinkRecord> currentBatch = result.get(result.size() - 1);
                currentBatch.add(recordBuilders.get(tp).build());
            }
            return result;
        });
    }

    private static class SinkRecordBuilder {
        private final Random random;
        private final RandomGenerator<String> keyGenerator;
        private final String topic;
        private final int partition;

        private int offset;

        private SinkRecordBuilder(final Random random,
                                  final RandomGenerator<String> keyGenerator,
                                  final String topic,
                                  final int partition) {
            this.random = random;
            this.keyGenerator = keyGenerator;
            this.topic = topic;
            this.partition = partition;
            // In a particular topic-partition, start from a random offset.
            this.offset = random.nextInt(MAX_START_OFFSET);
        }

        final SinkRecord build() {
            final String key = keyGenerator.next(random).value();
            final byte[] keyBytes;
            if (key != null) {
                keyBytes = key.getBytes(StandardCharsets.UTF_8);
            } else {
                keyBytes = null;
            }

            final String value = topic + "-" + partition;
            final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

            final SinkRecord record = new SinkRecord(
                    topic,
                    partition,
                    Schema.OPTIONAL_BYTES_SCHEMA,
                    keyBytes,
                    Schema.OPTIONAL_BYTES_SCHEMA,
                    valueBytes,
                    offset);
            // Imitate gaps in offsets.
            final int offsetIncrement = random.nextInt(MAX_OFFSET_INCREMENT - 1) + 1;
            offset += offsetIncrement;
            return record;
        }
    }

    private String createFilename(final SinkRecord record) {
        return PREFIX + record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset();
    }

    private int effectiveMaxRecordsPerFile(final Integer maxRecordsPerFile) {
        if (maxRecordsPerFile == null) {
            return Integer.MAX_VALUE;
        } else {
            return maxRecordsPerFile;
        }
    }
}
