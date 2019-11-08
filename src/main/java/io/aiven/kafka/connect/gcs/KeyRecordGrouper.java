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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.gcs.templating.Template;

/**
 * A {@link RecordGrouper} that groups records by key.
 *
 * <p>The class requires a filename template with {@code key} variable declared.
 *
 * <p>The class supports one record per file.
 */
final class KeyRecordGrouper implements RecordGrouper {
    private static final List<String> EXPECTED_VARIABLE_LIST = Arrays.asList(
        FilenameTemplateVariable.KEY.name
    );

    private final Template filenameTemplate;

    // One record pre file, but use List here for the compatibility with the interface.
    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    /**
     * A constructor.
     *
     * @param filenameTemplate the filename template.
     */
    public KeyRecordGrouper(final Template filenameTemplate) {
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
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record, "records cannot be null");

        final String filename = renderFilename(record);

        fileBuffers.putIfAbsent(filename, new ArrayList<>());

        // one record per file
        final List<SinkRecord> records = fileBuffers.get(filename);
        records.clear();
        records.add(record);
    }

    private String renderFilename(final SinkRecord record) {
        final String keyString;
        if (record.key() == null) {
            keyString = "null";
        } else if (record.keySchema().type() == Schema.Type.STRING) {
            keyString = (String) record.key();
        } else {
            keyString = record.key().toString();
        }

        return filenameTemplate.instance()
            .bindVariable(FilenameTemplateVariable.KEY.name, () -> keyString)
            .render();
    }

    @Override
    public void clear() {
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
