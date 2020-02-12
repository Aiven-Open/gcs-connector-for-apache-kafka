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

package io.aiven.kafka.connect.gcs.config;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public enum FilenameTemplateVariable {
    TOPIC("topic"),
    PARTITION("partition"),
    START_OFFSET(
        "start_offset",
        "padding",
        ImmutableSet.of(Boolean.TRUE.toString(), Boolean.FALSE.toString())
    ) {
        @Override
        public String parameterDescription() {
            return String.join(
                "=",
                String.join(
                    ":",
                    name,
                    parameterName
                ),
                String.join(
                    "|",
                    parameterValues)
            );
        }
    },
    KEY("key");

    public final String name;

    public final String parameterName;

    public final Set<String> parameterValues;

    private static final String UNKNOWN_PARAMETER_NAME = "_unknown_";

    public String parameterDescription() {
        return "";
    }

    FilenameTemplateVariable(final String name) {
        this(name, UNKNOWN_PARAMETER_NAME, Collections.emptySet());
    }

    FilenameTemplateVariable(final String name,
                             final String parameterName,
                             final Set<String> parameterValues) {
        this.name = name;
        this.parameterName = parameterName;
        this.parameterValues = parameterValues;
    }

    public static FilenameTemplateVariable of(final String name) {
        for (final FilenameTemplateVariable v : FilenameTemplateVariable.values()) {
            if (v.name.equals(name)) {
                return v;
            }
        }
        throw new IllegalArgumentException(
            String.format(
                "Unknown filename template variable: %s",
                name)
        );
    }

}
