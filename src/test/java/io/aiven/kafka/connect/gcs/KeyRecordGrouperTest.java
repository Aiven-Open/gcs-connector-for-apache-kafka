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

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.templating.Template;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class KeyRecordGrouperTest {

    private static final SinkRecord T0P0R0 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null, 0);
    private static final SinkRecord T0P0R1 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null, 1);
    private static final SinkRecord T0P0R2 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2);
    private static final SinkRecord T0P0R3 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 3);
    private static final SinkRecord T0P0R4 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null, 4);
    private static final SinkRecord T0P0R5 =
        new SinkRecord("topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null, 5);

    private static final SinkRecord T0P1R0 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "c", null, null, 10);
    private static final SinkRecord T0P1R1 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "b", null, null, 11);
    private static final SinkRecord T0P1R2 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 12);
    private static final SinkRecord T0P1R3 =
        new SinkRecord("topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "c", null, null, 13);

    private static final SinkRecord T1P1R0 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "d", null, null, 1000);
    private static final SinkRecord T1P1R1 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "d", null, null, 1001);
    private static final SinkRecord T1P1R2 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1002);
    private static final SinkRecord T1P1R3 =
        new SinkRecord("topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "a", null, null, 1003);

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "{{topic}}", "{{partition}}", "{{start_offset}}",
        "{{topic}}-{{partition}}", "{{topic}}-{{start_offset}}", "{{partition}}-{{start_offset}}",
        "{{topic}}-{{partition}}-{{start_offset}}",
        "{{topic}}-{{partition}}-{{start_offset}}-{{key}}"
    })
    final void incorrectFilenameTemplates(final String template) {
        final Template filenameTemplate = Template.of(template);
        assertFalse(KeyRecordGrouper.acceptsTemplate(filenameTemplate));
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new KeyRecordGrouper(filenameTemplate));
        assertEquals(
            "filenameTemplate must have set of variables {key}, but {"
                + String.join(",", filenameTemplate.variables())
                + "} was given",
            ex.getMessage());
    }

    @Test
    final void empty() {
        final Template filenameTemplate = Template.of("{{key}}");
        assertTrue(KeyRecordGrouper.acceptsTemplate(filenameTemplate));
        final KeyRecordGrouper grouper = new KeyRecordGrouper(filenameTemplate);
        assertThat(grouper.records(), anEmptyMap());
    }

    @Test
    final void eachKeyInSinglePartition() {
        final Template filenameTemplate = Template.of("{{key}}");
        assertTrue(KeyRecordGrouper.acceptsTemplate(filenameTemplate));
        final KeyRecordGrouper grouper = new KeyRecordGrouper(filenameTemplate);

        grouper.put(T0P0R0);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P0R3);
        grouper.put(T0P0R4);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records.keySet(), containsInAnyOrder("a", "b", "null"));
        assertThat(records.get("a"), contains(T0P0R4));
        assertThat(records.get("b"), contains(T0P0R5));
        assertThat(records.get("null"), contains(T0P0R3));
    }

    @Test
    final void keysInMultiplePartitions() {
        final Template filenameTemplate = Template.of("{{key}}");
        assertTrue(KeyRecordGrouper.acceptsTemplate(filenameTemplate));
        final KeyRecordGrouper grouper = new KeyRecordGrouper(filenameTemplate);

        grouper.put(T0P0R0);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P0R3);
        grouper.put(T0P0R4);
        grouper.put(T0P0R5);

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);
        grouper.put(T0P1R3);

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(records.keySet(), containsInAnyOrder("a", "b", "c", "d", "null"));
        assertThat(records.get("a"), contains(T1P1R3));
        assertThat(records.get("b"), contains(T0P1R1));
        assertThat(records.get("c"), contains(T0P1R3));
        assertThat(records.get("d"), contains(T1P1R1));
        assertThat(records.get("null"), contains(T1P1R2));
    }
}
