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

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.gcs.templating.Template;
import io.aiven.kafka.connect.gcs.templating.Template.Instance;


/**
 * A {@link RecordGrouper} that groups records by topic and partition.
 *
 * <p>The class requires a filename template with {@code topic}, {@code partition},
 * and {@code start_offset} variables declared.
 *
 * <p>The class supports limited and unlimited number of records in files.
 */
final class HourlyTopicPartitionRecordGrouper extends TopicPartitionRecordGrouper {
    private static final List<String> EXPECTED_VARIABLE_LIST = Arrays.asList(
        FilenameTemplateVariable.TOPIC.name,
        FilenameTemplateVariable.PARTITION.name,
        FilenameTemplateVariable.START_OFFSET.name,
        FilenameTemplateVariable.YEAR.name,
        FilenameTemplateVariable.MONTH.name,
        FilenameTemplateVariable.DAY.name,
        FilenameTemplateVariable.HOUR.name
    );


     /**
     * A constructor.
     *
     * @param filenameTemplate  the filename template.
     * @param maxRecordsPerFile the maximum number of records per file ({@code null} for unlimited).
     */
    public HourlyTopicPartitionRecordGrouper(final Template filenameTemplate, final Integer maxRecordsPerFile) {
        super(filenameTemplate, maxRecordsPerFile);
    }

    @Override
    protected Instance renderFilename(final TopicPartition tp, final SinkRecord headRecord) {
        final LocalDateTime now = LocalDateTime.now();

        return super.renderFilename(tp, headRecord)
            .bindVariable(FilenameTemplateVariable.YEAR.name, () -> Integer.toString(now.getYear()))
            .bindVariable(FilenameTemplateVariable.MONTH.name, () -> Integer.toString(now.getMonthValue()))
            .bindVariable(FilenameTemplateVariable.DAY.name, () -> Integer.toString(now.getDayOfMonth()))
            .bindVariable(FilenameTemplateVariable.HOUR.name, () -> Integer.toString(now.getHour()));
    }

        /**
     * Checks if the template is acceptable for this grouper.
     */
    static boolean acceptsTemplate(final Template filenameTemplate) {
        return new HashSet<>(EXPECTED_VARIABLE_LIST).equals(filenameTemplate.variablesSet());
    }
}
}
