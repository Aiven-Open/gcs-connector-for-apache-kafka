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

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.gcs.templating.Template;
import io.aiven.kafka.connect.gcs.templating.VariableTemplatePart;

import com.google.auth.oauth2.UserCredentials;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link GcsSinkConfig} class.
 */
final class GcsSinkConfigTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "{{topic}}", "{{partition}}", "{{start_offset}}",
        "{{topic}}-{{partition}}", "{{topic}}-{{start_offset}}", "{{partition}}-{{start_offset}}",
        "{{topic}}-{{partition}}-{{start_offset}}",
        "{{topic}}-{{partition}}-{{start_offset}}-{{key}}"
    })
    final void incorrectFilenameTemplatesForKey(final String template) {
        final Map<String, String> properties =
            ImmutableMap.of(
                GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG, template);
        assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "{{topic}}", "{{partition}}", "{{start_offset}}",
        "{{topic}}-{{partition}}", "{{topic}}-{{start_offset}}", "{{partition}}-{{start_offset}}",
        "{{topic}}-{{partition}}-{{start_offset}}-{{unknown}}"
    })
    final void incorrectFilenameTemplatesForTopicPartitionRecord(final String template) {
        final Map<String, String> properties =
            ImmutableMap.of(
                GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG, template);
        assertThrows(
            ConfigException.class,   
            () -> new GcsSinkConfig(properties)
        );
    }

    @Test
    void acceptMultipleParametersWithTheSameName() {
        final Map<String, String> properties =
            ImmutableMap.of(
                GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG,
                "{{topic}}-{{timestamp:unit=YYYY}}-"
                    + "{{timestamp:unit=MM}}-{{timestamp:unit=dd}}"
                    + "-{{partition}}-{{start_offset:padding=true}}.gz",
                "gcs.bucket.name", "asdasd"
            );
        final Template t = new GcsSinkConfig(properties).getFilenameTemplate();
        final String fileName = t.instance()
            .bindVariable("topic", () -> "a")
            .bindVariable("timestamp", VariableTemplatePart.Parameter::value)
            .bindVariable("partition", () -> "p")
            .bindVariable("start_offset", VariableTemplatePart.Parameter::value)
            .render();
        assertEquals("a-YYYY-MM-dd-p-true.gz", fileName);
    }

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
        assertEquals("test-bucket", config.getBucketName());
        assertEquals(CompressionType.NONE, config.getCompressionType());
        assertEquals("", config.getPrefix());
        assertEquals("a-b-c",
            config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render());
        assertIterableEquals(Collections.singleton(
            new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64)), config.getOutputFields());
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
        properties.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=YYYY}}.gz");
        properties.put("file.max.records", "42");
        properties.put("format.output.fields", "key,value,offset,timestamp");
        properties.put("format.output.fields.value.encoding", "base64");

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertDoesNotThrow(config::getCredentials);
        assertEquals("test-bucket", config.getBucketName());
        assertEquals(CompressionType.GZIP, config.getCompressionType());
        assertEquals(42, config.getMaxRecordsPerFile());
        assertEquals("test-prefix", config.getPrefix());
        assertEquals("a-b-c-d.gz",
            config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .bindVariable("timestamp", () -> "d")
                .render());
        assertIterableEquals(Arrays.asList(
            new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
            new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64),
            new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
            new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE)), config.getOutputFields());

        assertEquals(ZoneOffset.UTC, config.getFilenameTimezone());
        assertEquals(
            TimestampSource.WallclockTimestampSource.class,
            config.getFilenameTimestampSource().getClass()
        );

    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"none", "gzip"})
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
        assertEquals("Invalid value unsupported for configuration file.compression.type: "
                + "supported values are: 'none', 'gzip'",
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
        assertEquals("Invalid value [key, value, offset, timestamp, unsupported] "
                + "for configuration format.output.fields: "
                + "supported values are: 'key', 'value', 'offset', 'timestamp'",
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
        assertEquals("test-client-id", credentials.getClientId());
        assertEquals("test-client-secret", credentials.getClientSecret());
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
        assertEquals("test-client-id", credentials.getClientId());
        assertEquals("test-client-secret", credentials.getClientSecret());
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
        assertEquals("Invalid value " + longString + " for configuration gcs.bucket.name: "
                + "cannot be longer than 1024 characters",
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
        assertEquals("Invalid value .well-known/acme-challenge/something for configuration gcs.bucket.name: "
                + "cannot start with '.well-known/acme-challenge'",
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
        assertEquals("Invalid value -42 for configuration file.max.records: "
                + "must be a non-negative integer number",
            t.getMessage());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"none", "gzip"})
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
    void acceptFilenameTemplateVariablesParameters() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{start_offset:padding=true}}-{{partition}}-{{topic}}");
        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
            .instance()
            .bindVariable("topic", () -> "a")
            .bindVariable("partition", () -> "b")
            .bindVariable("start_offset", parameter -> {
                assertEquals("padding", parameter.name());
                assertTrue(parameter.asBoolean());
                return "c";
            })
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
        assertEquals("Invalid value  for configuration file.name.template: "
                + "unsupported set of template variables, "
                + "supported sets are: topic,partition,start_offset,timestamp; key",
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
        assertEquals("Invalid value {{ aaa }}{{ topic }}{{ partition }}{{ start_offset }} "
                + "for configuration file.name.template: "
                + "unsupported set of template variables, "
                + "supported sets are: topic,partition,start_offset,timestamp; key",
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
        assertEquals("Invalid value {{ partition }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, "
                + "supported sets are: topic,partition,start_offset,timestamp; key",
            t.getMessage());
    }

    @Test
    void wrongVariableParameterValue() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{start_offset:padding=FALSE}}-{{partition}}-{{topic}}");
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(
            "Invalid value {{start_offset:padding=FALSE}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "unsupported set of template variables parameters, "
                + "supported sets are: start_offset:padding=true|false,timestamp:unit=YYYY|MM|dd|HH", t.getMessage());
    }

    @Test
    void variableWithoutRequiredParameterValue() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{start_offset}}-{{partition}}-{{topic}}-{{timestamp}}");
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(
            "Invalid value {{start_offset}}-{{partition}}-{{topic}}-{{timestamp}} "
                + "for configuration file.name.template: "
                + "parameter unit is required for the the variable timestamp, "
                + "supported values are: YYYY|MM|dd|HH", t.getMessage());
    }

    @Test
    void wrongVariableWithoutParameter() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{start_offset:}}-{{partition}}-{{topic}}");
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(
            "Invalid value {{start_offset:}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "Wrong variable with parameter definition", t.getMessage());
    }

    @Test
    void noVariableWithParameter() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{:padding=true}}-{{partition}}-{{topic}}");
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(
            "Invalid value {{:padding=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "Variable name has't been set for template: {{:padding=true}}-{{partition}}-{{topic}}",
            t.getMessage()
        );
    }

    @Test
    void wrongVariableWithoutParameterValue() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{start_offset:padding=}}-{{partition}}-{{topic}}");
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(
            "Invalid value {{start_offset:padding=}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "Parameter value for variable `start_offset` and parameter `padding` has not been set",
            t.getMessage()
        );
    }

    @Test
    void wrongVariableWithoutParameterName() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{start_offset:=true}}-{{partition}}-{{topic}}");
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(
            "Invalid value {{start_offset:=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "Parameter name for variable `start_offset` has not been set", t.getMessage());
    }

    @Test
    void filenameTemplateNoPartition() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.name.template", "{{ topic }}{{ start_offset }}");

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals("Invalid value {{ topic }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, "
                + "supported sets are: topic,partition,start_offset,timestamp; key",
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
        assertEquals("Invalid value {{ topic }}{{ partition }} for configuration file.name.template: "
                + "unsupported set of template variables, "
                + "supported sets are: topic,partition,start_offset,timestamp; key",
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

    @Test
    void correctShortFilenameTimezone() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put(GcsSinkConfig.FILE_NAME_TIMESTAMP_TIMEZONE, "CET");

        final GcsSinkConfig c = new GcsSinkConfig(properties);
        assertEquals(ZoneId.of("CET"), c.getFilenameTimezone());
    }

    @Test
    void correctLongFilenameTimezone() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put(GcsSinkConfig.FILE_NAME_TIMESTAMP_TIMEZONE, "Europe/Berlin");

        final GcsSinkConfig c = new GcsSinkConfig(properties);
        assertEquals(ZoneId.of("Europe/Berlin"), c.getFilenameTimezone());
    }

    @Test
    void wrongFilenameTimestampSource() {

        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put(GcsSinkConfig.FILE_NAME_TIMESTAMP_TIMEZONE, "Europe/Berlin");
        properties.put(GcsSinkConfig.FILE_NAME_TIMESTAMP_SOURCE, "UNKNOWN_TIMESTAMP_SOURCE");

        final Throwable t =
            assertThrows(
                ConfigException.class,
                () -> new GcsSinkConfig(properties)
            );
        assertEquals(
            "Invalid value UNKNOWN_TIMESTAMP_SOURCE for configuration "
                + "file.name.timestamp.source: Unknown timestamp source: UNKNOWN_TIMESTAMP_SOURCE",
            t.getMessage()
        );

    }

    @Test
    void correctFilenameTimestampSource() {

        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put(GcsSinkConfig.FILE_NAME_TIMESTAMP_TIMEZONE, "Europe/Berlin");
        properties.put(GcsSinkConfig.FILE_NAME_TIMESTAMP_SOURCE, "wallclock");

        final GcsSinkConfig c = new GcsSinkConfig(properties);
        assertEquals(TimestampSource.WallclockTimestampSource.class, c.getFilenameTimestampSource().getClass());

    }

}
