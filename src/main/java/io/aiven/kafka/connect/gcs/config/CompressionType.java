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

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public enum CompressionType {
    NONE("none"),
    GZIP("gzip");

    public final String name;

    CompressionType(final String name) {
        this.name = name;
    }

    public static CompressionType forName(final String name) {
        Objects.requireNonNull(name);

        if (NONE.name.equalsIgnoreCase(name)) {
            return NONE;
        } else if (GZIP.name.equalsIgnoreCase(name)) {
            return GZIP;
        } else {
            throw new IllegalArgumentException("Unknown compression type: " + name);
        }
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }
}
