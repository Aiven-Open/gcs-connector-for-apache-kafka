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

package io.aiven.kafka.connect.common.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public enum CompressionType {
    NONE("none") {
        @Override
        public String extension() {
            return "";
        }
    },
    GZIP("gzip") {
        @Override
        public String extension() {
            return ".gz";
        }
    },
    SNAPPY("snappy") {
        @Override
        public String extension() {
            return ".snappy";
        }
    },
    ZSTD("zstd") {
        @Override
        public String extension() {
            return ".zst";
        }
    };

    public final String name;

    CompressionType(final String name) {
        this.name = name;

    }

    public static CompressionType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");

        if (NONE.name.equalsIgnoreCase(name)) {
            return NONE;
        }
        if (GZIP.name.equalsIgnoreCase(name)) {
            return GZIP;
        }
        if (SNAPPY.name.equalsIgnoreCase(name)) {
            return SNAPPY;
        }
        if (ZSTD.name.equalsIgnoreCase(name)) {
            return ZSTD;
        }
        throw new IllegalArgumentException("Unknown compression type: " + name);
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }

    public abstract String extension();
}
