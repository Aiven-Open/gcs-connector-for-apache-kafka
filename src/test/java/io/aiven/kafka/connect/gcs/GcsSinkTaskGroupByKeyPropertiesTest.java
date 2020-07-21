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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.config.GcsSinkConfig;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This is a property-based test for {@link GcsSinkTask} (grouping records by the key)
 * using <a href="https://jqwik.net/docs/current/user-guide.html">jqwik</a>.
 *
 * <p>The idea is to generate random batches of {@link SinkRecord}
 * (see {@link PbtBase#recordBatches()}, put them into a task, and check certain properties
 * of the written files afterwards. Files are written virtually using the in-memory GCS mock.
 */
final class GcsSinkTaskGroupByKeyPropertiesTest extends PbtBase {

    @Property
    final void groupByKey(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches) {
        final Storage storage = LocalStorageHelper.getOptions().getService();
        final BucketAccessor testBucketAccessor = new BucketAccessor(storage, TEST_BUCKET, true);

        final Map<String, String> taskProps = basicTaskProps();
        taskProps.put(GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG, "{{key}}");
        taskProps.put(GcsSinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, "none");
        final GcsSinkTask task = new GcsSinkTask(taskProps, storage);

        for (final List<SinkRecord> recordBatch : recordBatches) {
            task.put(recordBatch);
            task.flush(null);
        }

        final Map<String, SinkRecord> lastRecordPerKey = new HashMap<>();
        for (final List<SinkRecord> recordBatch : recordBatches) {
            for (final SinkRecord record : recordBatch) {
                lastRecordPerKey.put((String) record.key(), record);
            }
        }

        // Check expected file names.
        final List<String> expectedFileNames = lastRecordPerKey.keySet().stream()
            .map(this::createFilename)
            .collect(Collectors.toList());
        assertThat(testBucketAccessor.getBlobNames(), containsInAnyOrder(expectedFileNames.toArray()));

        // Check file contents.
        for (final String key : lastRecordPerKey.keySet()) {
            final SinkRecord record = lastRecordPerKey.get(key);
            final String filename = createFilename(key);

            final List<String> lines = testBucketAccessor.readLines(filename, "none");
            assertThat(lines, hasSize(1));

            final String expectedKeySubstring;
            if (record.key() == null) {
                expectedKeySubstring = "";
            } else {
                final String keyStr = (String) record.key();
                expectedKeySubstring = Base64.getEncoder().encodeToString(keyStr.getBytes());
            }
            final String expectedValueSubstring = new String((byte[]) record.value(), StandardCharsets.UTF_8);
            final String expectedLine = String.format("%s,%s,%d",
                expectedKeySubstring, expectedValueSubstring, record.kafkaOffset());
            assertEquals(expectedLine, lines.get(0));
        }
    }

    private String createFilename(final String key) {
        return PREFIX + key;
    }
}
