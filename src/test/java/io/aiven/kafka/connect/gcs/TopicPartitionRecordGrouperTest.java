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

import io.aiven.kafka.connect.gcs.templating.Template;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TopicPartitionRecordGrouperTest {

    private static final SinkRecord T0P0R0 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 0);
    private static final SinkRecord T0P0R1 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1);
    private static final SinkRecord T0P0R2 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2);
    private static final SinkRecord T0P0R3 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 3);
    private static final SinkRecord T0P0R4 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 4);
    private static final SinkRecord T0P0R5 = new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 5);

    private static final SinkRecord T0P1R0 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 10);
    private static final SinkRecord T0P1R1 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 11);
    private static final SinkRecord T0P1R2 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 12);
    private static final SinkRecord T0P1R3 = new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 13);

    private static final SinkRecord T1P1R0 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1000);
    private static final SinkRecord T1P1R1 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1001);
    private static final SinkRecord T1P1R2 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1002);
    private static final SinkRecord T1P1R3 = new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1003);

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "{{topic}}", "{{partition}}", "{{start_offset}}",
            "{{topic}}-{{partition}}", "{{topic}}-{{start_offset}}", "{{partition}}-{{start_offset}}",
            "{{topic}}-{{partition}}-{{start_offset}}-{{unknown}}"
    })
    final void incorrectFilenameTemplates(final String template) {
        final Template filenameTemplate = new Template(template);
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new TopicPartitionRecordGrouper(filenameTemplate, null));
        assertEquals(
                "filenameTemplate must have set of variables {topic,partition,start_offset}, but {"
                + String.join(",", filenameTemplate.getVariables())
                + "} was given",
                ex.getMessage());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = 10)
    final void empty(final Integer maxRecordsPerFile) {
        final Template filenameTemplate = new Template("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper fileGrouper = new TopicPartitionRecordGrouper(filenameTemplate, maxRecordsPerFile);
        assertThat(fileGrouper.records(), anEmptyMap());
    }

    @Test
    final void unlimited() {
        final Template filenameTemplate = new Template("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper fileGrouper = new TopicPartitionRecordGrouper(filenameTemplate, null);

        fileGrouper.put(T0P1R0);
        fileGrouper.put(T0P0R0);
        fileGrouper.put(T0P1R1);
        fileGrouper.put(T0P0R1);
        fileGrouper.put(T0P0R2);
        fileGrouper.put(T0P1R2);
        fileGrouper.put(T0P0R3);
        fileGrouper.put(T1P1R0);
        fileGrouper.put(T1P1R1);
        fileGrouper.put(T0P0R4);
        fileGrouper.put(T1P1R2);
        fileGrouper.put(T1P1R3);
        fileGrouper.put(T0P0R5);
        fileGrouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = fileGrouper.records();
        assertThat(records.keySet(), containsInAnyOrder("topic0-0-0", "topic0-1-10", "topic1-0-1000"));
        assertThat(records.get("topic0-0-0"),
                contains(T0P0R0, T0P0R1, T0P0R2, T0P0R3, T0P0R4, T0P0R5));
        assertThat(records.get("topic0-1-10"),
                contains(T0P1R0, T0P1R1, T0P1R2, T0P1R3));
        assertThat(records.get("topic1-0-1000"),
                contains(T1P1R0, T1P1R1, T1P1R2, T1P1R3));
    }

    @Test
    final void limited() {
        final Template filenameTemplate = new Template("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper fileGrouper = new TopicPartitionRecordGrouper(filenameTemplate, 2);

        fileGrouper.put(T0P1R0);
        fileGrouper.put(T0P0R0);
        fileGrouper.put(T0P1R1);
        fileGrouper.put(T0P0R1);
        fileGrouper.put(T0P0R2);
        fileGrouper.put(T0P1R2);
        fileGrouper.put(T0P0R3);
        fileGrouper.put(T1P1R0);
        fileGrouper.put(T1P1R1);
        fileGrouper.put(T0P0R4);
        fileGrouper.put(T1P1R2);
        fileGrouper.put(T1P1R3);
        fileGrouper.put(T0P0R5);
        fileGrouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = fileGrouper.records();
        assertThat(records.keySet(),
                containsInAnyOrder("topic0-0-0", "topic0-0-2", "topic0-0-4", "topic0-1-10", "topic0-1-12", "topic1-0-1000", "topic1-0-1002"));
        assertThat(records.get("topic0-0-0"),
                contains(T0P0R0, T0P0R1));
        assertThat(records.get("topic0-0-2"),
                contains(T0P0R2, T0P0R3));
        assertThat(records.get("topic0-0-4"),
                contains(T0P0R4, T0P0R5));
        assertThat(records.get("topic0-1-10"),
                contains(T0P1R0, T0P1R1));
        assertThat(records.get("topic0-1-12"),
                contains(T0P1R2, T0P1R3));
        assertThat(records.get("topic1-0-1000"),
                contains(T1P1R0, T1P1R1));
        assertThat(records.get("topic1-0-1002"),
                contains(T1P1R2, T1P1R3));
    }

    @Test
    final void clear() {
        final Template filenameTemplate = new Template("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper fileGrouper = new TopicPartitionRecordGrouper(filenameTemplate, null);

        fileGrouper.put(T0P1R0);
        fileGrouper.put(T0P0R0);
        fileGrouper.put(T0P1R1);
        fileGrouper.put(T0P0R1);
        fileGrouper.put(T0P0R2);
        fileGrouper.put(T0P1R2);
        fileGrouper.put(T0P0R3);
        fileGrouper.put(T0P1R3);

        fileGrouper.clear();
        assertThat(fileGrouper.records(), anEmptyMap());

        fileGrouper.put(T1P1R0);
        fileGrouper.put(T1P1R1);
        fileGrouper.put(T0P0R4);
        fileGrouper.put(T1P1R2);
        fileGrouper.put(T1P1R3);
        fileGrouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = fileGrouper.records();
        assertThat(records.keySet(), containsInAnyOrder("topic0-0-4", "topic1-0-1000"));
        assertThat(records.get("topic0-0-4"),
                contains(T0P0R4, T0P0R5));
        assertThat(records.get("topic1-0-1000"),
                contains(T1P1R0, T1P1R1, T1P1R2, T1P1R3));
    }
}
