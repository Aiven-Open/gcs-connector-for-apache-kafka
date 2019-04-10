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

package io.aiven.kafka.connect.gcs.config;

import com.google.auth.oauth2.UserCredentials;
import com.google.common.io.Resources;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests {@link GcsSinkConfig} class.
 */
final class GcsSinkConfigTest {

    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = new HashMap<>();
        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Missing required configuration \"gcs.bucket.name\" which has no default value.", t.getMessage());
    }

    @Test
    void emptyGcsBucketName() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "");
        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value  for configuration gcs.bucket.name: String must be non-empty", t.getMessage());
    }

    @Test
    void correctMinimalConfig() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertAll(
                () -> assertEquals("test-bucket", config.getBucketName()),
                () -> assertEquals(CompressionType.NONE, config.getCompressionType()),
                () -> assertEquals("", config.getPrefix()),
                () -> assertIterableEquals(Collections.singleton(OutputField.VALUE), config.getOutputFields())
        );
    }

    @Test
    void correctFullConfig() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(
                "gcs.credentials.path",
                getClass().getClassLoader().getResource("test_gcs_credentials.json").getPath());
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.compression.type", "gzip");
        properties.put("file.name.prefix", "test-prefix");
        properties.put("format.output.fields", "key,value,offset,timestamp");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertAll(
                () -> assertDoesNotThrow(() -> config.getCredentials()),
                () -> assertEquals("test-bucket", config.getBucketName()),
                () -> assertEquals(CompressionType.GZIP, config.getCompressionType()),
                () -> assertEquals("test-prefix", config.getPrefix()),
                () -> assertIterableEquals(Arrays.asList(
                        OutputField.KEY,
                        OutputField.VALUE,
                        OutputField.OFFSET,
                        OutputField.TIMESTAMP), config.getOutputFields())
        );
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "none", "gzip" })
    void supportedCompression(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final CompressionType expectedCompressionType;
        if (compression == null || "none".equals(compression)) {
            expectedCompressionType = CompressionType.NONE;
        } else if ("gzip".equals(compression)) {
            expectedCompressionType = CompressionType.GZIP;
        } else {
            throw new RuntimeException("Shouldn't be here");
        }
        assertEquals(expectedCompressionType, config.getCompressionType());
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.compression.type", "unsupported");

        final Throwable t = assertThrows(
                ConfigException.class, () -> new GcsSinkConfig(properties)
        );
        assertEquals("Invalid value unsupported for configuration file.compression.type: " +
                "supported values are: 'none', 'gzip'",
                t.getMessage());
    }

    @Test
    void emptyOutputField() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("format.output.fields", "");

        final Throwable t = assertThrows(
                ConfigException.class, () -> new GcsSinkConfig(properties)
        );
        assertEquals("Invalid value [] for configuration format.output.fields: cannot be empty",
                t.getMessage());
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("format.output.fields", "key,value,offset,timestamp,unsupported");

        final Throwable t = assertThrows(
                ConfigException.class, () -> new GcsSinkConfig(properties)
        );
        assertEquals("Invalid value [key, value, offset, timestamp, unsupported] " +
                        "for configuration format.output.fields: " +
                        "supported values are: 'key', 'value', 'offset', 'timestamp'",
                t.getMessage());
    }

    @Test
    void gcsCredentialsPath() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put(
                "gcs.credentials.path",
                getClass().getClassLoader().getResource("test_gcs_credentials.json").getPath());

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final UserCredentials credentials = (UserCredentials) config.getCredentials();
        assertAll(
                () -> assertEquals("test-client-id", credentials.getClientId()),
                () -> assertEquals("test-client-secret", credentials.getClientSecret())
        );
    }

    @Test
    void gcsCredentialsJson() throws IOException {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");

        final String credentialsJson = Resources.toString(
                getClass().getClassLoader().getResource("test_gcs_credentials.json"),
                StandardCharsets.UTF_8
        );
        properties.put("gcs.credentials.json", credentialsJson);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final UserCredentials credentials = (UserCredentials) config.getCredentials();
        assertAll(
                () -> assertEquals("test-client-id", credentials.getClientId()),
                () -> assertEquals("test-client-secret", credentials.getClientSecret())
        );
    }

    @Test
    void gcsCredentialsExclusivity() throws IOException {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");

        final URL credentialsResource = getClass().getClassLoader().getResource("test_gcs_credentials.json");
        final String credentialsJson = Resources.toString(credentialsResource, StandardCharsets.UTF_8);
        properties.put("gcs.credentials.json", credentialsJson);
        properties.put("gcs.credentials.path", credentialsResource.getPath());

        final Throwable t = assertThrows(ConfigException.class, () -> new GcsSinkConfig(properties));
        assertEquals(
                "\"gcs.credentials.path\" and \"gcs.credentials.json\" are mutually exclusive options, but both are set.",
                t.getMessage());
    }

    @Test
    void connectorName() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("name", "test-connector");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertEquals("test-connector", config.getConnectorName());
    }

    @Test
    void fileNamePrefixTooLong() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        final String longString = Stream.generate(() -> "a").limit(1025).collect(Collectors.joining());
        properties.put("file.name.prefix", longString);
        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value " + longString + " for configuration gcs.bucket.name: " +
                "cannot be longer than 1024 characters",
                t.getMessage());
    }

    @Test
    void fileNamePrefixProhibitedPrefix() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.prefix", ".well-known/acme-challenge/something");
        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value .well-known/acme-challenge/something for configuration gcs.bucket.name: " +
                "cannot start with '.well-known/acme-challenge'",
                t.getMessage());
    }
}
