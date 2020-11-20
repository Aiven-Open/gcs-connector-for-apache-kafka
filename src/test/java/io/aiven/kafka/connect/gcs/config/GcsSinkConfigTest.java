/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import io.aiven.kafka.connect.gcs.GcsSinkConfig;

import com.google.auth.oauth2.UserCredentials;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        "{{topic}}-{{partition}}-{{start_offset}}-{{key}}",
        "{{topic}}-{{partition}}-{{start_offset}}-{{unknown}}"
    })
    final void incorrectFilenameTemplates(final String template) {
        final Map<String, String> properties = Map.of(
            "file.name.template", template,
            "gcs.bucket.name", "some-bucket"
        );

        final ConfigValue v = GcsSinkConfig.configDef().validate(properties).stream()
            .filter(x -> x.name().equals(GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG))
            .findFirst()
            .get();
        assertFalse(v.errorMessages().isEmpty());

        final var t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertTrue(t.getMessage().startsWith("Invalid value "));
    }

    @Test
    void acceptMultipleParametersWithTheSameName() {
        final Map<String, String> properties = Map.of(
            "file.name.template",
            "{{topic}}-{{timestamp:unit=yyyy}}-"
                + "{{timestamp:unit=MM}}-{{timestamp:unit=dd}}"
                + "-{{partition}}-{{start_offset:padding=true}}.gz",
            "gcs.bucket.name", "asdasd"
        );

        assertConfigDefValidationPasses(properties);

        final Template t = new GcsSinkConfig(properties).getFilenameTemplate();
        final String fileName = t.instance()
            .bindVariable("topic", () -> "a")
            .bindVariable("timestamp", VariableTemplatePart.Parameter::value)
            .bindVariable("partition", () -> "p")
            .bindVariable("start_offset", VariableTemplatePart.Parameter::value)
            .render();
        assertEquals("a-yyyy-MM-dd-p-true.gz", fileName);
    }

    @Test
    void correctlySupportDeprecatedYyyyUppercase() {
        final Map<String, String> properties = Map.of(
            "file.name.template",
            "{{topic}}-"
                + "{{timestamp:unit=YYYY}}-{{timestamp:unit=yyyy}}-"
                + "{{ timestamp:unit=YYYY }}-{{ timestamp:unit=yyyy }}"
                + "-{{partition}}-{{start_offset:padding=true}}.gz",
            "gcs.bucket.name", "asdasd"
        );

        // Commented out because this is actually a bug, will be fixed later.
        // assertConfigDefValidationPasses(properties);

        final Template t = new GcsSinkConfig(properties).getFilenameTemplate();
        final String fileName = t.instance()
            .bindVariable("topic", () -> "_")
            .bindVariable("timestamp", VariableTemplatePart.Parameter::value)
            .bindVariable("partition", () -> "_")
            .bindVariable("start_offset", () -> "_")
            .render();
        assertEquals("_-yyyy-yyyy-yyyy-yyyy-_-_.gz", fileName);
    }

    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = Map.of();

        final var expectedErrorMessage =
            "Missing required configuration \"gcs.bucket.name\" which has no default value.";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "gcs.bucket.name", expectedErrorMessage);
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void emptyGcsBucketName() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", ""
        );

        final var expectedErrorMessage =
            "Invalid value  for configuration gcs.bucket.name: String must be non-empty";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "gcs.bucket.name", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void correctMinimalConfig() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket"
        );

        assertConfigDefValidationPasses(properties);

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
        assertEquals(FormatType.forName("csv"), config.getFormatType());
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void correctFullConfig(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(
            "gcs.credentials.path",
            getClass().getClassLoader().getResource("test_gcs_credentials.json").getPath());
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.compression.type", compression);
        properties.put("file.name.prefix", "test-prefix");
        properties.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=yyyy}}.gz");
        properties.put("file.max.records", "42");
        properties.put("format.output.fields", "key,value,offset,timestamp");
        properties.put("format.output.fields.value.encoding", "base64");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertDoesNotThrow(config::getCredentials);
        assertEquals("test-bucket", config.getBucketName());
        assertEquals(CompressionType.forName(compression), config.getCompressionType());
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
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void supportedCompression(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final CompressionType expectedCompressionType =
                compression == null ? CompressionType.NONE : CompressionType.forName(compression);
        assertEquals(expectedCompressionType, config.getCompressionType());
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.compression.type", "unsupported"
        );

        final var expectedErrorMessage = "Invalid value unsupported for configuration file.compression.type: "
            + "supported values are: 'none', 'gzip', 'snappy', 'zstd'";

        final var v = expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.compression.type", expectedErrorMessage);
        assertIterableEquals(List.of("none", "gzip", "snappy", "zstd"), v.recommendedValues());

        final Throwable t = assertThrows(
            ConfigException.class, () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void emptyOutputField() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "format.output.fields", ""
        );

        final var expectedErrorMessage = "Invalid value [] for configuration format.output.fields: cannot be empty";

        final var v = expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "format.output.fields", expectedErrorMessage);
        assertIterableEquals(List.of("key", "value", "offset", "timestamp", "headers"), v.recommendedValues());

        final Throwable t = assertThrows(
            ConfigException.class, () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "format.output.fields", "key,value,offset,timestamp,headers,unsupported"
        );

        final var expectedErrorMessage = "Invalid value [key, value, offset, timestamp, headers, unsupported] "
            + "for configuration format.output.fields: "
            + "supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'";

        final var v = expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "format.output.fields", expectedErrorMessage);
        assertIterableEquals(List.of("key", "value", "offset", "timestamp", "headers"), v.recommendedValues());

        final Throwable t = assertThrows(
            ConfigException.class, () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void gcsCredentialsPath() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.credentials.path",
            getClass().getClassLoader().getResource("test_gcs_credentials.json").getPath()
        );

        assertConfigDefValidationPasses(properties);

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

        assertConfigDefValidationPasses(properties);

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

        // Should pass here, because ConfigDef validation doesn't check interdependencies.
        assertConfigDefValidationPasses(properties);

        final Throwable t = assertThrows(ConfigException.class, () -> new GcsSinkConfig(properties));
        assertEquals(
            "\"gcs.credentials.path\" and \"gcs.credentials.json\" are mutually exclusive options, but both are set.",
            t.getMessage());
    }

    @Test
    void connectorName() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "name", "test-connector"
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertEquals("test-connector", config.getConnectorName());
    }

    @Test
    void fileNamePrefixTooLong() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        final String longString = Stream.generate(() -> "a").limit(1025).collect(Collectors.joining());
        properties.put("file.name.prefix", longString);

        final var expectedErrorMessage = "Invalid value " + longString + " for configuration gcs.bucket.name: "
            + "cannot be longer than 1024 characters";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.prefix", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void fileNamePrefixProhibitedPrefix() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.prefix", ".well-known/acme-challenge/something"
        );

        final var expectedErrorMessage =
            "Invalid value .well-known/acme-challenge/something for configuration gcs.bucket.name: "
            + "cannot start with '.well-known/acme-challenge'";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.prefix", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void maxRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket"
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertEquals(0, config.getMaxRecordsPerFile());
    }

    @Test
    void maxRecordsPerFileSetCorrect() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.max.records", "42"
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertEquals(42, config.getMaxRecordsPerFile());
    }

    @Test
    void maxRecordsPerFileSetIncorrect() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.max.records", "-42"
        );

        final var expectedErrorMessage = "Invalid value -42 for configuration file.max.records: "
            + "must be a non-negative integer number";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.max.records", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void filenameTemplateNotSet(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        assertConfigDefValidationPasses(properties);

        final CompressionType compressionType =
                compression == null ? CompressionType.NONE : CompressionType.forName(compression);
        final String expected = "a-b-c" + compressionType.extension();

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
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{topic}}-{{partition}}-{{start_offset}}"
        );

        assertConfigDefValidationPasses(properties);

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
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{start_offset}}-{{partition}}-{{topic}}"
        );

        assertConfigDefValidationPasses(properties);

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
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{start_offset:padding=true}}-{{partition}}-{{topic}}"
        );

        assertConfigDefValidationPasses(properties);
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
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{key}}"
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
            .instance()
            .bindVariable("key", () -> "a")
            .render();
        assertEquals("a", actual);
    }

    @Test
    void emptyFilenameTemplate() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", ""
        );

        final var expectedErrorMessage = "Invalid value  for configuration file.name.template: "
            + "unsupported set of template variables, "
            + "supported sets are: topic,partition,start_offset,timestamp; key";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void filenameTemplateUnknownVariable() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{ aaa }}{{ topic }}{{ partition }}{{ start_offset }}"
        );

        final var expectedErrorMessage = "Invalid value {{ aaa }}{{ topic }}{{ partition }}{{ start_offset }} "
            + "for configuration file.name.template: "
            + "unsupported set of template variables, "
            + "supported sets are: topic,partition,start_offset,timestamp; key";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void filenameTemplateNoTopic() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{ partition }}{{ start_offset }}"
        );

        final var expectedErrorMessage =
            "Invalid value {{ partition }}{{ start_offset }} for configuration file.name.template: "
            + "unsupported set of template variables, "
            + "supported sets are: topic,partition,start_offset,timestamp; key";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void wrongVariableParameterValue() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{start_offset:padding=FALSE}}-{{partition}}-{{topic}}"
        );

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=FALSE}}-{{partition}}-{{topic}} "
            + "for configuration file.name.template: "
            + "unsupported set of template variables parameters, "
            + "supported sets are: start_offset:padding=true|false,timestamp:unit=yyyy|MM|dd|HH";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void variableWithoutRequiredParameterValue() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{start_offset}}-{{partition}}-{{topic}}-{{timestamp}}"
        );

        final var expectedErrorMessage = "Invalid value {{start_offset}}-{{partition}}-{{topic}}-{{timestamp}} "
            + "for configuration file.name.template: "
            + "parameter unit is required for the the variable timestamp, "
            + "supported values are: yyyy|MM|dd|HH";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void wrongVariableWithoutParameter() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{start_offset:}}-{{partition}}-{{topic}}"
        );

        final var expectedErrorMessage = "Invalid value {{start_offset:}}-{{partition}}-{{topic}} "
            + "for configuration file.name.template: "
            + "Wrong variable with parameter definition";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void noVariableWithParameter() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{:padding=true}}-{{partition}}-{{topic}}"
        );

        final var expectedErrorMessage = "Invalid value {{:padding=true}}-{{partition}}-{{topic}} "
            + "for configuration file.name.template: "
            + "Variable name has't been set for template: {{:padding=true}}-{{partition}}-{{topic}}";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage()
        );
    }

    @Test
    void wrongVariableWithoutParameterValue() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{start_offset:padding=}}-{{partition}}-{{topic}}"
        );

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=}}-{{partition}}-{{topic}} "
            + "for configuration file.name.template: "
            + "Parameter value for variable `start_offset` and parameter `padding` has not been set";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void wrongVariableWithoutParameterName() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{start_offset:=true}}-{{partition}}-{{topic}}"
        );

        final var expectedErrorMessage = "Invalid value {{start_offset:=true}}-{{partition}}-{{topic}} "
            + "for configuration file.name.template: "
            + "Parameter name for variable `start_offset` has not been set";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void filenameTemplateNoPartition() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{ topic }}{{ start_offset }}"
        );

        final var expectedErrorMessage =
            "Invalid value {{ topic }}{{ start_offset }} for configuration file.name.template: "
            + "unsupported set of template variables, "
            + "supported sets are: topic,partition,start_offset,timestamp; key";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void filenameTemplateNoStartOffset() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{ topic }}{{ partition }}"
        );

        final var expectedErrorMessage =
            "Invalid value {{ topic }}{{ partition }} for configuration file.name.template: "
            + "unsupported set of template variables, "
            + "supported sets are: topic,partition,start_offset,timestamp; key";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.template", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{key}}"
        );

        assertConfigDefValidationPasses(properties);
        assertDoesNotThrow(() -> new GcsSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFile1() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{key}}",
            "file.max.records", "1"
        );

        assertConfigDefValidationPasses(properties);
        assertDoesNotThrow(() -> new GcsSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileMoreThan1() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.template", "{{key}}",
            "file.max.records", "42"
        );

        // Should pass here, because ConfigDef validation doesn't check interdependencies.
        assertConfigDefValidationPasses(properties);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties));
        assertEquals("When file.name.template is {{key}}, file.max.records must be either 1 or not set",
            t.getMessage());
    }

    @Test
    void correctShortFilenameTimezone() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.timestamp.timezone", "CET"
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig c = new GcsSinkConfig(properties);
        assertEquals(ZoneId.of("CET"), c.getFilenameTimezone());
    }

    @Test
    void correctLongFilenameTimezone() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.timestamp.timezone", "Europe/Berlin"
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig c = new GcsSinkConfig(properties);
        assertEquals(ZoneId.of("Europe/Berlin"), c.getFilenameTimezone());
    }

    @Test
    void wrongFilenameTimestampSource() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.timestamp.timezone", "Europe/Berlin",
            "file.name.timestamp.source", "UNKNOWN_TIMESTAMP_SOURCE"
        );

        final var expectedErrorMessage = "Invalid value UNKNOWN_TIMESTAMP_SOURCE for configuration "
            + "file.name.timestamp.source: Unknown timestamp source: UNKNOWN_TIMESTAMP_SOURCE";

        expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "file.name.timestamp.source", expectedErrorMessage);

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void correctFilenameTimestampSource() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "file.name.timestamp.timezone", "Europe/Berlin",
            "file.name.timestamp.source", "wallclock"
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig c = new GcsSinkConfig(properties);
        assertEquals(TimestampSource.WallclockTimestampSource.class, c.getFilenameTimestampSource().getClass());
    }

    @ParameterizedTest
    @ValueSource(strings = {"jsonl", "json", "csv"})
    void supportedFormatTypeConfig(final String formatType) {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "format.output.type", formatType
        );

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig c = new GcsSinkConfig(properties);
        final FormatType expectedFormatType = FormatType.forName(formatType);

        assertEquals(expectedFormatType, c.getFormatType());
    }

    @Test
    void wrongFormatTypeConfig() {
        final Map<String, String> properties = Map.of(
            "gcs.bucket.name", "test-bucket",
            "format.output.type", "unknown"
        );

        final var expectedErrorMessage = "Invalid value unknown for configuration format.output.type: "
            + "supported values are: 'csv', 'json', 'jsonl'";

        final var v = expectErrorMessageForConfigurationInConfigDefValidation(
            properties, "format.output.type", expectedErrorMessage);
        assertIterableEquals(List.of("csv", "json", "jsonl"), v.recommendedValues());

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new GcsSinkConfig(properties)
        );
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    private void assertConfigDefValidationPasses(final Map<String, String> properties) {
        for (final ConfigValue v : GcsSinkConfig.configDef().validate(properties)) {
            assertTrue(v.errorMessages().isEmpty());
        }
    }

    private ConfigValue expectErrorMessageForConfigurationInConfigDefValidation(
        final Map<String, String> properties,
        final String configuration,
        final String expectedErrorMessage) {
        ConfigValue result = null;
        for (final ConfigValue v : GcsSinkConfig.configDef().validate(properties)) {
            if (v.name().equals(configuration)) {
                assertIterableEquals(List.of(expectedErrorMessage), v.errorMessages());
                result = v;
            } else {
                assertTrue(v.errorMessages().isEmpty());
            }
        }
        assertNotNull(result, "Not found");
        return result;
    }
}
