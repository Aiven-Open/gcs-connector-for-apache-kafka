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
                () -> assertEquals("a-b-c",
                        config.getFilenameTemplate()
                                .instance()
                                .bindVariable("topic", () -> "a")
                                .bindVariable("partition", () -> "b")
                                .bindVariable("start_offset", () -> "c")
                                .render()),
                () -> assertIterableEquals(Collections.singleton(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64)), config.getOutputFields())
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
        properties.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}.gz");
        properties.put("file.max.records", "42");
        properties.put("format.output.fields", "key,value,offset,timestamp");
        properties.put("format.output.fields.value.encoding", "base64");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertAll(
                () -> assertDoesNotThrow(() -> config.getCredentials()),
                () -> assertEquals("test-bucket", config.getBucketName()),
                () -> assertEquals(CompressionType.GZIP, config.getCompressionType()),
                () -> assertEquals(42, config.getMaxRecordsPerFile()),
                () -> assertEquals("test-prefix", config.getPrefix()),
                () -> assertEquals("a-b-c.gz",
                        config.getFilenameTemplate()
                                .instance()
                                .bindVariable("topic", () -> "a")
                                .bindVariable("partition", () -> "b")
                                .bindVariable("start_offset", () -> "c")
                                .render()),
                () -> assertIterableEquals(Arrays.asList(
                        new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64),
                        new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE)), config.getOutputFields())
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

    @Test
    void maxRecordsPerFileNotSet() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertEquals(0, config.getMaxRecordsPerFile());
    }

    @Test
    void maxRecordsPerFileSetCorrect() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.max.records", "42");
        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertEquals(42, config.getMaxRecordsPerFile());
    }

    @Test
    void maxRecordsPerFileSetIncorrect() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.max.records", "-42");
        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value -42 for configuration file.max.records: " +
                        "must be a non-negative integer number",
                t.getMessage());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "none", "gzip" })
    void filenameTemplateNotSet(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        String expected = "a-b-c";
        if ("gzip".equals(compression)) {
            expected += ".gz";
        }

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render();
        assertEquals(expected, actual);
    }

    @Test
    void topicPartitionOffsetFilenameTemplateVariablesOrder1() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render();
        assertEquals("a-b-c", actual);
    }

    @Test
    void topicPartitionOffsetFilenameTemplateVariablesOrder2() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{start_offset}}-{{partition}}-{{topic}}");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render();
        assertEquals("c-b-a", actual);
    }

    @Test
    void keyFilenameTemplateVariable() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{key}}");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("key", () -> "a")
                .render();
        assertEquals("a", actual);
    }

    @Test
    void emptyFilenameTemplate() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "");

        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value  for configuration file.name.template: " +
                        "unsupported set of template variables, supported sets are: topic,partition,start_offset; key",
                t.getMessage());
    }

    @Test
    void filenameTemplateUnknownVariable() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{ aaa }}{{ topic }}{{ partition }}{{ start_offset }}");

        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value {{ aaa }}{{ topic }}{{ partition }}{{ start_offset }} for configuration file.name.template: " +
                        "unsupported set of template variables, supported sets are: topic,partition,start_offset; key",
                t.getMessage());
    }

    @Test
    void filenameTemplateNoTopic() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{ partition }}{{ start_offset }}");

        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value {{ partition }}{{ start_offset }} for configuration file.name.template: " +
                        "unsupported set of template variables, supported sets are: topic,partition,start_offset; key",
                t.getMessage());
    }

    @Test
    void filenameTemplateNoPartition() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{ topic }}{{ start_offset }}");

        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value {{ topic }}{{ start_offset }} for configuration file.name.template: " +
                        "unsupported set of template variables, supported sets are: topic,partition,start_offset; key",
                t.getMessage());
    }

    @Test
    void filenameTemplateNoStartOffset() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{ topic }}{{ partition }}");

        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value {{ topic }}{{ partition }} for configuration file.name.template: " +
                        "unsupported set of template variables, supported sets are: topic,partition,start_offset; key",
                t.getMessage());
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileNotSet() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{key}}");

        assertDoesNotThrow(() -> new GcsSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFile1() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{key}}");
        properties.put("file.max.records", "1");

        assertDoesNotThrow(() -> new GcsSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileMoreThan1() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{key}}");
        properties.put("file.max.records", "42");

        final Throwable t = assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties));
        assertEquals("When file.name.template is {{key}}, file.max.records must be either 1 or not set",
                t.getMessage());
    }
}
