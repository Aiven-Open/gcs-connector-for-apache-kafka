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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.gcs.templating.Template;

/**
 * A {@link RecordGrouper} that groups records by topic and partition.
 *
 * <p>The class requires a filename template with {@code topic}, {@code partition},
 * and {@code start_offset} variables declared.
 *
 * <p>The class supports limited and unlimited number of records in files.
 */
final class TopicPartitionRecordGrouper implements RecordGrouper {
    private static final List<String> EXPECTED_VARIABLE_LIST = Arrays.asList(
        FilenameTemplateVariable.TOPIC.name,
        FilenameTemplateVariable.PARTITION.name,
        FilenameTemplateVariable.START_OFFSET.name
    );

    private final Template filenameTemplate;

    private final Integer maxRecordsPerFile;

    private final Map<TopicPartition, SinkRecord> currentHeadRecords = new HashMap<>();

    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    /**
     * A constructor.
     *
     * @param filenameTemplate  the filename template.
     * @param maxRecordsPerFile the maximum number of records per file ({@code null} for unlimited).
     */
    public TopicPartitionRecordGrouper(final Template filenameTemplate, final Integer maxRecordsPerFile) {
        Objects.requireNonNull(filenameTemplate, "filenameTemplate cannot be null");

        if (!acceptsTemplate(filenameTemplate)) {
            throw new IllegalArgumentException(
                "filenameTemplate must have set of variables {"
                    + String.join(",", EXPECTED_VARIABLE_LIST)
                    + "}, but {"
                    + String.join(",", filenameTemplate.variables())
                    + "} was given"
            );
        }

        this.filenameTemplate = filenameTemplate;
        this.maxRecordsPerFile = maxRecordsPerFile;
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record, "record cannot be null");

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
            .bindVariable(
                FilenameTemplateVariable.PARTITION.name,
                () -> Integer.toString(tp.partition())
            ).bindVariable(
                FilenameTemplateVariable.START_OFFSET.name,
                usePaddingParameter -> usePaddingParameter.asBoolean()
                    ? String.format("%020d", headRecord.kafkaOffset())
                    : Long.toString(headRecord.kafkaOffset())
            ).render();
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

    /**
     * Checks if the template is acceptable for this grouper.
     */
    static boolean acceptsTemplate(final Template filenameTemplate) {
        return new HashSet<>(EXPECTED_VARIABLE_LIST).equals(filenameTemplate.variablesSet());
    }
}
