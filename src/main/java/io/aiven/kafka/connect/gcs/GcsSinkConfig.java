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

package io.aiven.kafka.connect.gcs;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FilenameTemplateValidator;
import io.aiven.kafka.connect.common.config.FixedSetRecommender;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.templating.Template;

import com.google.auth.oauth2.GoogleCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkConfig extends AivenCommonConfig {
    private static final Logger log = LoggerFactory.getLogger(GcsSinkConfig.class);

    private static final String GROUP_GCS = "GCS";
    public static final String GCS_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    public static final String GCS_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";

    private static final String GROUP_FILE = "File";
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";

    private static final String GROUP_FORMAT = "Format";
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";

    public static final String NAME_CONFIG = "name";

    private static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";

    public static ConfigDef configDef() {
        final GcsSinkConfigDef configDef = new GcsSinkConfigDef();
        addGcsConfigGroup(configDef);
        addFileConfigGroup(configDef);
        addFormatConfigGroup(configDef);
        return configDef;
    }

    private static void addGcsConfigGroup(final ConfigDef configDef) {
        int gcsGroupCounter = 0;
        configDef.define(
            GCS_CREDENTIALS_PATH_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "The path to a GCP credentials file. "
                + "If not provided, the connector will try to detect the credentials automatically. "
                + "Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG + "\"",
            GROUP_GCS,
            gcsGroupCounter++,
            ConfigDef.Width.NONE,
            GCS_CREDENTIALS_PATH_CONFIG
        );

        configDef.define(
            GCS_CREDENTIALS_JSON_CONFIG,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.LOW,
            "GCP credentials as a JSON string. "
                + "If not provided, the connector will try to detect the credentials automatically. "
                + "Cannot be set together with \"" + GCS_CREDENTIALS_PATH_CONFIG + "\"",
            GROUP_GCS,
            gcsGroupCounter++,
            ConfigDef.Width.NONE,
            GCS_CREDENTIALS_JSON_CONFIG
        );

        configDef.define(
            GCS_BUCKET_NAME_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            "The GCS bucket name to store output files in.",
            GROUP_GCS,
            gcsGroupCounter++,
            ConfigDef.Width.NONE,
            GCS_BUCKET_NAME_CONFIG
        );
    }

    private static void addFileConfigGroup(final ConfigDef configDef) {
        int fileGroupCounter = 0;
        configDef.define(
            FILE_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    // See https://cloud.google.com/storage/docs/naming
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (valueStr.length() > 1024) {
                        throw new ConfigException(GCS_BUCKET_NAME_CONFIG, value,
                            "cannot be longer than 1024 characters");
                    }
                    if (valueStr.startsWith(".well-known/acme-challenge")) {
                        throw new ConfigException(GCS_BUCKET_NAME_CONFIG, value,
                            "cannot start with '.well-known/acme-challenge'");
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The prefix to be added to the name of each file put on GCS.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.NONE,
            FILE_NAME_PREFIX_CONFIG
        );

        configDef.define(
            FILE_NAME_TEMPLATE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG),
            ConfigDef.Importance.MEDIUM,
            "The template for file names on GCS. "
                + "Supports `{{ variable }}` placeholders for substituting variables. "
                + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                + "(the offset of the first record in the file). "
                + "Only some combinations of variables are valid, which currently are:\n"
                + "- `topic`, `partition`, `start_offset`.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.LONG,
            FILE_NAME_TEMPLATE_CONFIG
        );

        final String supportedCompressionTypes = CompressionType.names().stream()
            .map(f -> "'" + f + "'")
            .collect(Collectors.joining(", "));
        configDef.define(
            FILE_COMPRESSION_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            CompressionType.NONE.name,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (!CompressionType.names().contains(valueStr)) {
                        throw new ConfigException(
                            FILE_COMPRESSION_TYPE_CONFIG, valueStr,
                            "supported values are: " + supportedCompressionTypes);
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The compression type used for files put on GCS. "
                + "The supported values are: " + supportedCompressionTypes + ".",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.NONE,
            FILE_COMPRESSION_TYPE_CONFIG,
            FixedSetRecommender.ofSupportedValues(CompressionType.names())
        );

        configDef.define(
            FILE_MAX_RECORDS,
            ConfigDef.Type.INT,
            0,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof Integer;
                    if ((Integer) value < 0) {
                        throw new ConfigException(
                            FILE_MAX_RECORDS, value,
                            "must be a non-negative integer number");
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The maximum number of records to put in a single file. "
                + "Must be a non-negative integer number. "
                + "0 is interpreted as \"unlimited\", which is the default.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_MAX_RECORDS
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_TIMEZONE,
            ConfigDef.Type.STRING,
            ZoneOffset.UTC.toString(),
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    try {
                        ZoneId.of(value.toString());
                    } catch (final Exception e) {
                        throw new ConfigException(
                            FILE_NAME_TIMESTAMP_TIMEZONE,
                            value,
                            e.getMessage());
                    }
                }
            },
            ConfigDef.Importance.LOW,
            "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                + "Use standard shot and long names. Default is UTC",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_TIMEZONE
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_SOURCE,
            ConfigDef.Type.STRING,
            TimestampSource.Type.WALLCLOCK.name(),
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    try {
                        TimestampSource.Type.of(value.toString());
                    } catch (final Exception e) {
                        throw new ConfigException(
                            FILE_NAME_TIMESTAMP_SOURCE,
                            value,
                            e.getMessage());
                    }
                }
            },
            ConfigDef.Importance.LOW,
            "Specifies the the timestamp variable source. Default is wall-clock.",
            GROUP_FILE,
            fileGroupCounter,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_SOURCE
        );

    }

    private static void addFormatConfigGroup(final ConfigDef configDef) {
        int formatGroupCounter = 0;

        addFormatTypeConfig(configDef, formatGroupCounter);

        final String supportedOutputFields = OutputFieldType.names().stream()
            .map(f -> "'" + f + "'")
            .collect(Collectors.joining(", "));
        configDef.define(
            FORMAT_OUTPUT_FIELDS_CONFIG,
            ConfigDef.Type.LIST,
            OutputFieldType.VALUE.name,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof List;
                    @SuppressWarnings("unchecked") final List<String> valueList = (List<String>) value;
                    if (valueList.isEmpty()) {
                        throw new ConfigException(
                            FORMAT_OUTPUT_FIELDS_CONFIG, valueList,
                            "cannot be empty");
                    }
                    for (final String fieldName : valueList) {
                        if (!OutputFieldType.isValidName(fieldName)) {
                            throw new ConfigException(
                                FORMAT_OUTPUT_FIELDS_CONFIG, value,
                                "supported values are: " + supportedOutputFields);
                        }
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "Fields to put into output files. "
                + "The supported values are: " + supportedOutputFields + ".",
            GROUP_FORMAT,
            formatGroupCounter++,
            ConfigDef.Width.NONE,
            FORMAT_OUTPUT_FIELDS_CONFIG,
            FixedSetRecommender.ofSupportedValues(OutputFieldType.names())
        );

        final String supportedValueFieldEncodingTypes = CompressionType.names().stream()
            .map(f -> "'" + f + "'")
            .collect(Collectors.joining(", "));
        configDef.define(
            FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
            ConfigDef.Type.STRING,
            OutputFieldEncodingType.BASE64.name,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (!OutputFieldEncodingType.names().contains(valueStr)) {
                        throw new ConfigException(
                            FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, valueStr,
                            "supported values are: " + supportedValueFieldEncodingTypes);
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The type of encoding for the value field. "
                + "The supported values are: " + supportedOutputFields + ".",
            GROUP_FORMAT,
            formatGroupCounter++,
            ConfigDef.Width.NONE,
            FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
            FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names())
        );
    }

    public GcsSinkConfig(final Map<String, String> properties) {
        super(configDef(), handleDeprecatedYyyyUppercase(properties));
        validate();
    }

    static Map<String, String> handleDeprecatedYyyyUppercase(final Map<String, String> properties) {
        if (properties.containsKey(FILE_NAME_TEMPLATE_CONFIG)) {
            final var result = new HashMap<>(properties);

            String template = properties.get(FILE_NAME_TEMPLATE_CONFIG);
            final String originalTemplate = template;

            final var unitYyyyPattern = Pattern.compile("\\{\\{\\s*timestamp\\s*:\\s*unit\\s*=\\s*YYYY\\s*}}");
            template = unitYyyyPattern.matcher(template)
                .replaceAll(matchResult -> matchResult.group().replace("YYYY", "yyyy"));

            if (!template.equals(originalTemplate)) {
                log.warn("{{timestamp:unit=YYYY}} is no longer supported, "
                        + "please use {{timestamp:unit=yyyy}} instead. "
                        + "It was automatically replaced: {}",
                    template);
            }

            result.put(FILE_NAME_TEMPLATE_CONFIG, template);

            return result;
        } else {
            return properties;
        }
    }

    private void validate() {
        final String credentialsPath = getString(GCS_CREDENTIALS_PATH_CONFIG);
        final Password credentialsJson = getPassword(GCS_CREDENTIALS_JSON_CONFIG);
        if (credentialsPath != null && credentialsJson != null) {
            final String msg = String.format(
                "\"%s\" and \"%s\" are mutually exclusive options, but both are set.",
                GCS_CREDENTIALS_PATH_CONFIG, GCS_CREDENTIALS_JSON_CONFIG);
            throw new ConfigException(msg);
        }

        // Special checks for {{key}} filename template.
        final Template filenameTemplate = getFilenameTemplate();
        if (RecordGrouperFactory.KEY_RECORD.equals(RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate))) {
            if (getMaxRecordsPerFile() > 1) {
                final String msg = String.format("When %s is %s, %s must be either 1 or not set",
                    FILE_NAME_TEMPLATE_CONFIG, filenameTemplate, FILE_MAX_RECORDS);
                throw new ConfigException(msg);
            }
        }

    }

    public final GoogleCredentials getCredentials() {
        final String credentialsPath = getString(GCS_CREDENTIALS_PATH_CONFIG);
        final Password credentialsJsonPwd = getPassword(GCS_CREDENTIALS_JSON_CONFIG);
        try {
            String credentialsJson = null;
            if (credentialsJsonPwd != null) {
                credentialsJson = credentialsJsonPwd.value();
            }
            return GoogleCredentialsBuilder.build(credentialsPath, credentialsJson);
        } catch (final Exception e) {
            throw new ConfigException("Failed to create GCS credentials: " + e.getMessage());
        }
    }

    public final String getBucketName() {
        return getString(GCS_BUCKET_NAME_CONFIG);
    }

    public final CompressionType getCompressionType() {
        return CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG));
    }

    public final List<OutputField> getOutputFields() {
        final List<OutputField> result = new ArrayList<>();
        for (final String outputFieldTypeStr : getList(FORMAT_OUTPUT_FIELDS_CONFIG)) {
            final OutputFieldType fieldType = OutputFieldType.forName(outputFieldTypeStr);
            final OutputFieldEncodingType encodingType;
            if (fieldType == OutputFieldType.VALUE) {
                encodingType = OutputFieldEncodingType.forName(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG));
            } else {
                encodingType = OutputFieldEncodingType.NONE;
            }
            result.add(new OutputField(fieldType, encodingType));
        }
        return result;
    }

    public final String getPrefix() {
        return getString(FILE_NAME_PREFIX_CONFIG);
    }

    public final String getConnectorName() {
        return originalsStrings().get(NAME_CONFIG);
    }

    private String resolveFilenameTemplate() {
        String fileNameTemplate = getString(FILE_NAME_TEMPLATE_CONFIG);
        if (fileNameTemplate == null) {
            fileNameTemplate = DEFAULT_FILENAME_TEMPLATE + getCompressionType().extension();
        }
        return fileNameTemplate;
    }

}
