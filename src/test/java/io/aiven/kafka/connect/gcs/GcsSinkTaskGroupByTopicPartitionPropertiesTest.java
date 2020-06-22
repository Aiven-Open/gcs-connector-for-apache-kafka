/*
 * Aiven Kafka GCS Connector
 * Copyright (c) 2019 Aiven Oy
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.config.GcsSinkConfig;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.Lists;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a property-based test for {@link GcsSinkTask} (grouping records by the topic and partition)
 * using <a href="https://jqwik.net/docs/current/user-guide.html">jqwik</a>.
 *
 * <p>The idea is to generate random batches of {@link SinkRecord}
 * (see {@link PbtBase#recordBatches()}, put them into a task, and check certain properties
 * of the written files afterwards. Files are written virtually using the in-memory GCS mock.
 */
final class GcsSinkTaskGroupByTopicPartitionPropertiesTest extends PbtBase {

    @Property
    final void unlimited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches) {
        genericTry(recordBatches, null);
    }

    @Property
    final void limited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches,
                       @ForAll @IntRange(min = 1, max = 100) final int maxRecordsPerFile) {
        genericTry(recordBatches, maxRecordsPerFile);
    }

    private void genericTry(final List<List<SinkRecord>> recordBatches,
                            final Integer maxRecordsPerFile) {
        final Storage storage = LocalStorageHelper.getOptions().getService();
        final BucketAccessor testBucketAccessor = new BucketAccessor(storage, TEST_BUCKET, true);

        final Map<String, String> taskProps = basicTaskProps();
        if (maxRecordsPerFile != null) {
            taskProps.put(GcsSinkConfig.FILE_MAX_RECORDS, Integer.toString(maxRecordsPerFile));
        }
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
                bucketAccessor.readLines(filename, "none"),
                hasSize(allOf(greaterThan(0), lessThanOrEqualTo(effectiveMax)))
            );
        }
    }

    /**
     * Checks, that:
     * <ul>
     * <li>the total number of records written to all files is correct;</li>
     * <li>each record is written only once.</li>
     * </ul>
     */
    private void checkTotalRecordCountAndNoMultipleWrites(final int expectedCount,
                                                          final BucketAccessor bucketAccessor) {
        final Set<String> seenRecords = new HashSet<>();
        for (final String filename : bucketAccessor.getBlobNames()) {
            for (final String line : bucketAccessor.readLines(filename, "none")) {
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

            final List<List<String>> lines = bucketAccessor
                    .readAndDecodeLines(filename, "none", FIELD_KEY, FIELD_VALUE);
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
            final List<List<String>> lines = bucketAccessor
                    .readAndDecodeLines(filename, "none", FIELD_KEY, FIELD_VALUE);
            final List<Integer> offsets = lines.stream()
                .map(line -> Integer.parseInt(line.get(FIELD_OFFSET)))
                .collect(Collectors.toList());
            for (int i = 0; i < offsets.size() - 1; i++) {
                assertTrue(offsets.get(i) < offsets.get(i + 1));
            }
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
