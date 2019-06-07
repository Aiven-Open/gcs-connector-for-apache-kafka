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

import io.aiven.kafka.connect.gcs.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.gcs.templating.Template;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;

/**
 * A {@link RecordGrouper} that groups record by topic and partition.
 *
 * <p>The class requires a filename template with {@code topic}, {@code partition},
 * and {@code start_offset} variables declared.
 *
 * <p>The class supports limited and unlimited number of records in files.
 */
final class TopicPartitionRecordGrouper implements RecordGrouper {
    private static final List<String> EXPECTED_VARIABLE_LIST = new ArrayList<>();
    static {
        EXPECTED_VARIABLE_LIST.add(FilenameTemplateVariable.TOPIC.name);
        EXPECTED_VARIABLE_LIST.add(FilenameTemplateVariable.PARTITION.name);
        EXPECTED_VARIABLE_LIST.add(FilenameTemplateVariable.START_OFFSET.name);
    }
    private static final Set<String> EXPECTED_VARIABLE_SET = new HashSet<>(EXPECTED_VARIABLE_LIST);

    private final Template filenameTemplate;
    private final Integer maxRecordsPerFile;

    private final Map<TopicPartition, SinkRecord> currentHeadRecords = new HashMap<>();
    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    /**
     * A constructor.
     *
     * @param filenameTemplate the filename template.
     * @param maxRecordsPerFile the maximum number of records per file ({@code null} for unlimited).
     */
    public TopicPartitionRecordGrouper(final Template filenameTemplate, final Integer maxRecordsPerFile) {
        Objects.requireNonNull(filenameTemplate);

        if (!EXPECTED_VARIABLE_SET.equals(new HashSet<>(filenameTemplate.getVariables()))) {
            throw new IllegalArgumentException(
                    "filenameTemplate must have set of variables {"
                            + String.join(",", EXPECTED_VARIABLE_LIST)
                            + "}, but {"
                            + String.join(",", filenameTemplate.getVariables())
                            + "} was given"
                    );
        }

        this.filenameTemplate = filenameTemplate;
        this.maxRecordsPerFile = maxRecordsPerFile;
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record);

        final TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        final SinkRecord currentHeadRecord = currentHeadRecords.computeIfAbsent(tp, ignored -> record);
        final String filename = renderFilename(tp, currentHeadRecord);

        if (shouldCreateNewFile(filename)) {
            // Create new file using this record as the head record.
            currentHeadRecords.put(tp, record);
            final String newFilename = renderFilename(tp, record);
            fileBuffers.computeIfAbsent(newFilename, ignored -> new ArrayList<>()).add(record);
        } else {
            fileBuffers.computeIfAbsent(filename, ignored -> new ArrayList<>()).add(record);
        }
    }

    private String renderFilename(final TopicPartition tp, final SinkRecord headRecord) {
        return filenameTemplate.instance()
                .bindVariable(FilenameTemplateVariable.TOPIC.name, tp::topic)
                .bindVariable(FilenameTemplateVariable.PARTITION.name, () -> Integer.toString(tp.partition()))
                .bindVariable(FilenameTemplateVariable.START_OFFSET.name, () -> Long.toString(headRecord.kafkaOffset()))
                .render();
    }

    private boolean shouldCreateNewFile(final String filename) {
        final boolean unlimited = maxRecordsPerFile == null;
        if (unlimited) {
            return false;
        } else {
            final List<SinkRecord> buffer = fileBuffers.get(filename);
            return buffer == null || buffer.size() >= maxRecordsPerFile;
        }
    }

    @Override
    public void clear() {
        currentHeadRecords.clear();
        fileBuffers.clear();
    }

    @Override
    public Map<String, List<SinkRecord>> records() {
        return Collections.unmodifiableMap(fileBuffers);
    }
}
