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

import com.google.auth.oauth2.GoogleCredentials;
import io.aiven.kafka.connect.gcs.gcs.GoogleCredentialsBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class GcsSinkConfig extends AbstractConfig {
    private static final String GROUP_GCS = "GCS";
    public static final String GCS_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    public static final String GCS_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";

    private static final String GROUP_FILE = "File";
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";

    private static final String GROUP_FORMAT = "Format";
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";

    public static final String NAME_CONFIG = "name";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();
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
                "The path to a GCP credentials file. " +
                        "If not provided, the connector will try to detect the credentials automatically. " +
                        "Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG + "\"",
                GROUP_GCS,
                gcsGroupCounter++,
                ConfigDef.Width.NONE,
                GCS_CREDENTIALS_PATH_CONFIG
        );

        configDef.define(
                GCS_CREDENTIALS_JSON_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                "GCP credentials as a JSON string. " +
                        "If not provided, the connector will try to detect the credentials automatically. " +
                        "Cannot be set together with \"" + GCS_CREDENTIALS_PATH_CONFIG + "\"",
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
                "The compression type used for files put on GCS. " +
                        "The supported values are: " + supportedCompressionTypes + ".",
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
                "The maximum number of records to put in a single file. " +
                        "Must be a non-negative integer number. " +
                        "0 is interpreted as \"unlimited\", which is the default.",
                GROUP_FILE,
                fileGroupCounter++,
                ConfigDef.Width.SHORT,
                FILE_MAX_RECORDS
        );
    }

    private static void addFormatConfigGroup(final ConfigDef configDef) {
        int formatGroupCounter = 0;

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
                        @SuppressWarnings("unchecked")
                        final List<String> valueList = (List<String>) value;
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
                "Fields to put into output files. " +
                        "The supported values are: " + supportedOutputFields + ".",
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
                "The type of encoding for the value field. " +
                        "The supported values are: " + supportedOutputFields + ".",
                GROUP_FORMAT,
                formatGroupCounter++,
                ConfigDef.Width.NONE,
                FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names())
        );
    }

    public GcsSinkConfig(final Map<String, String> properties) {
        super(configDef(), properties);
        validate();
    }

    private void validate() {
        final String credentialsPath = getString(GCS_CREDENTIALS_PATH_CONFIG);
        final String credentialsJson = getString(GCS_CREDENTIALS_JSON_CONFIG);
        if (credentialsPath != null && credentialsJson != null) {
            final String msg = String.format(
                    "\"%s\" and \"%s\" are mutually exclusive options, but both are set.",
                    GCS_CREDENTIALS_PATH_CONFIG, GCS_CREDENTIALS_JSON_CONFIG);
            throw new ConfigException(msg);
        }
    }

    public final GoogleCredentials getCredentials() {
        final String credentialsPath = getString(GCS_CREDENTIALS_PATH_CONFIG);
        final String credentialsJson = getString(GCS_CREDENTIALS_JSON_CONFIG);
        try {
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

    public final boolean isMaxRecordPerFileLimited() {
        return getMaxRecordsPerFile() > 0;
    }

    public final int getMaxRecordsPerFile() {
        return getInt(FILE_MAX_RECORDS);
    }
}
