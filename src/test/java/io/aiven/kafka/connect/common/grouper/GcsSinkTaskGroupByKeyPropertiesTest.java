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

package io.aiven.kafka.connect.common.grouper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.GcsSinkConfig;
import io.aiven.kafka.connect.gcs.GcsSinkTask;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

/**
 * This is a property-based test for {@link GcsSinkTask} (grouping records by the key) using
 * <a href="https://jqwik.net/docs/current/user-guide.html">jqwik</a>.
 *
 * <p>
 * The idea is to generate random batches of {@link SinkRecord} (see {@link PbtBase#recordBatches()}, put them into a
 * task, and check certain properties of the written files afterwards. Files are written virtually using the in-memory
 * GCS mock.
 */
final class GcsSinkTaskGroupByKeyPropertiesTest extends PbtBase {

    @Property
    void groupByKey(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches) {
        final Storage storage = LocalStorageHelper.getOptions().getService(); // NOPMD No need to close
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
        final List<String> expectedFileNames = lastRecordPerKey.keySet()
                .stream()
                .map(this::createFilename)
                .collect(Collectors.toList());
        assertThat(testBucketAccessor.getBlobNames(), containsInAnyOrder(expectedFileNames.toArray()));

        // Check file contents.
        for (final Map.Entry<String, SinkRecord> entry : lastRecordPerKey.entrySet()) {
            final SinkRecord record = lastRecordPerKey.get(entry.getKey());
            final String filename = createFilename(entry.getKey());

            final List<String> lines = testBucketAccessor.readLines(filename, "none");
            assertThat(lines, hasSize(1));

            final String expectedKeySubstring;
            if (record.key() == null) {
                expectedKeySubstring = "";
            } else {
                final String keyStr = (String) record.key();
                expectedKeySubstring = Base64.getEncoder().encodeToString(keyStr.getBytes(StandardCharsets.UTF_8));
            }
            final String expectedValueSubstring = new String((byte[]) record.value(), StandardCharsets.UTF_8); // NOPMD
                                                                                                               // instantiation
                                                                                                               // in a
                                                                                                               // loop
            final String expectedLine = String.format("%s,%s,%d", expectedKeySubstring, expectedValueSubstring,
                    record.kafkaOffset());
            assertEquals(expectedLine, lines.get(0));
        }
    }

    private String createFilename(final String key) {
        return PREFIX + key;
    }
}
