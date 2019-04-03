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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class GcsSinkConfig extends AbstractConfig {
    public static final String GCS_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    public static final String GCS_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";

    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";

    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";

    public static final String NAME_CONFIG = "name";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();
        configDef.define(
                GCS_CREDENTIALS_PATH_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                "The path to a GCP credentials file. " +
                        "If not provided, the connector will try to detect the credentials automatically. " +
                        "Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG + "\""
        );

        configDef.define(
                GCS_CREDENTIALS_JSON_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                "GCP credentials as a JSON string. " +
                        "If not provided, the connector will try to detect the credentials automatically. " +
                        "Cannot be set together with \"" + GCS_CREDENTIALS_PATH_CONFIG + "\""
        );

        configDef.define(
                GCS_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "The GCS bucket name to store output files in."
        );

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
                "The prefix to be added to the name of each file put on GCS."
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
                        "The supported values are: " + supportedCompressionTypes + "."
        );

        final String supportedOutputFields = OutputField.names().stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(
                FORMAT_OUTPUT_FIELDS_CONFIG,
                ConfigDef.Type.LIST,
                OutputField.VALUE.name,
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
                            if (!OutputField.isValidName(fieldName)) {
                                throw new ConfigException(
                                        FORMAT_OUTPUT_FIELDS_CONFIG, value,
                                        "supported values are: " + supportedOutputFields);
                            }
                        }
                    }
                },
                ConfigDef.Importance.MEDIUM,
                "Fields to put into output files. " +
                        "The supported values are: " + supportedOutputFields + "."
        );
        return configDef;
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
        return getList(FORMAT_OUTPUT_FIELDS_CONFIG).stream()
                .map(OutputField::forName)
                .collect(Collectors.toList());
    }

    public final String getPrefix() {
        return getString(FILE_NAME_PREFIX_CONFIG);
    }

    public final String getConnectorName() {
        return originalsStrings().get(NAME_CONFIG);
    }
}
