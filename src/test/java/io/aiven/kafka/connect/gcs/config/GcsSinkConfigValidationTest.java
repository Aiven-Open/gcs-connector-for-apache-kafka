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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * Tests {@link GcsSinkConfig}'s config definition.
 */
final class GcsSinkConfigValidationTest {

    @Test
    void recommendedValuesForCompression() {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("file.compression.type", "unknown");

        final ConfigValue v = GcsSinkConfig.configDef().validate(connectorProps).stream()
            .filter(x -> x.name().equals("file.compression.type"))
            .findFirst()
            .get();
        assertIterableEquals(
            CompressionType.names(),
            v.recommendedValues()
        );
    }

    @Test
    void recommendedValuesForFields() {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("format.output.fields", "unknown");

        final ConfigValue v = GcsSinkConfig.configDef().validate(connectorProps).stream()
            .filter(x -> x.name().equals("format.output.fields"))
            .findFirst()
            .get();
        assertIterableEquals(
            OutputFieldType.names(),
            v.recommendedValues()
        );
    }
}
