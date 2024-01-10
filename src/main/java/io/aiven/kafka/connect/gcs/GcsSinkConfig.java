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
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FixedSetRecommender;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.validators.FilenameTemplateValidator;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public final class GcsSinkConfig extends AivenCommonConfig {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkConfig.class);
    private static final String USER_AGENT_HEADER_FORMAT = "Google GCS Sink/%s (GPN: Aiven;)";
    public static final String USER_AGENT_HEADER_VALUE = String.format(USER_AGENT_HEADER_FORMAT, Version.VERSION);
    private static final String GROUP_GCS = "GCS";
    public static final String GCS_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    public static final String GCS_ENDPOINT_CONFIG = "gcs.endpoint";
    public static final String GCS_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    public static final String GCS_CREDENTIALS_DEFAULT_CONFIG = "gcs.credentials.default";
    public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";
    public static final String GCS_USER_AGENT = "gcs.user.agent";
    private static final String GROUP_FILE = "File";
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";

    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";

    private static final String GROUP_GCS_RETRY_BACKOFF_POLICY = "GCS retry backoff policy";

    public static final String GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG = "gcs.retry.backoff.initial.delay.ms";

    public static final String GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "gcs.retry.backoff.max.delay.ms";

    public static final String GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG = "gcs.retry.backoff.delay.multiplier";

    public static final String GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG = "gcs.retry.backoff.total.timeout.ms";

    public static final String GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG = "gcs.retry.backoff.max.attempts";

    // All default from GCS client, hardcoded here since GCS hadn't constants
    public static final long GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT = 1_000L;

    public static final long GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 32_000L;

    public static final double GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT = 2.0D;

    public static final long GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_DEFAULT = 50_000L;

    public static final int GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT = 6;

    public static final String NAME_CONFIG = "name";

    // the maximum number of allowable credential configurations that can be defined at a single time.
    private static final int MAX_ALLOWED_CREDENTIAL_CONFIGS = 1;

    public static ConfigDef configDef() {
        final GcsSinkConfigDef configDef = new GcsSinkConfigDef();
        addGcsConfigGroup(configDef);
        addFileConfigGroup(configDef);
        addOutputFieldsFormatConfigGroup(configDef, OutputFieldType.VALUE);
        addKafkaBackoffPolicy(configDef);
        addGcsRetryPolicies(configDef);
        addUserAgentConfig(configDef);
        return configDef;
    }

    private static void addUserAgentConfig(final ConfigDef configDef) {
        configDef.define(GCS_USER_AGENT, ConfigDef.Type.STRING, USER_AGENT_HEADER_VALUE, ConfigDef.Importance.LOW,
                "A custom user agent used while contacting google");
    }

    private static void addGcsConfigGroup(final ConfigDef configDef) {
        int gcsGroupCounter = 0;
        configDef.define(GCS_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                "Explicit GCS Endpoint Address, mainly for testing", GROUP_GCS, gcsGroupCounter++, ConfigDef.Width.NONE,
                GCS_ENDPOINT_CONFIG);
        configDef.define(GCS_CREDENTIALS_PATH_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                "The path to a GCP credentials file. Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG
                        + " or \"" + GCS_CREDENTIALS_DEFAULT_CONFIG + "\"",
                GROUP_GCS, gcsGroupCounter++, ConfigDef.Width.NONE, GCS_CREDENTIALS_PATH_CONFIG);

        configDef.define(GCS_CREDENTIALS_JSON_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.LOW,
                "GCP credentials as a JSON string. Cannot be set together with \"" + GCS_CREDENTIALS_PATH_CONFIG
                        + " or \"" + GCS_CREDENTIALS_DEFAULT_CONFIG + "\"",
                GROUP_GCS, gcsGroupCounter++, ConfigDef.Width.NONE, GCS_CREDENTIALS_JSON_CONFIG);

        configDef.define(GCS_CREDENTIALS_DEFAULT_CONFIG, ConfigDef.Type.BOOLEAN, null, ConfigDef.Importance.LOW,
                "Whether to connect using default the GCP SDK default credential discovery. When set to"
                        + "null (the default) or false, will fall back to connecting with No Credentials."
                        + "Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG + "\" or \""
                        + GCS_CREDENTIALS_PATH_CONFIG + "\"",
                GROUP_GCS, gcsGroupCounter++, ConfigDef.Width.NONE, GCS_CREDENTIALS_DEFAULT_CONFIG);

        configDef.define(GCS_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                "The GCS bucket name to store output files in.", GROUP_GCS, gcsGroupCounter++, ConfigDef.Width.NONE, // NOPMD
                                                                                                                     // the
                                                                                                                     // gcsGroupCounter
                                                                                                                     // updated
                                                                                                                     // value
                                                                                                                     // never
                                                                                                                     // used
                GCS_BUCKET_NAME_CONFIG);
    }

    private static void addGcsRetryPolicies(final ConfigDef configDef) {
        var retryPolicyGroupCounter = 0;
        configDef.define(GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Initial retry delay in milliseconds. The default value is "
                        + GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT,
                GROUP_GCS_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Maximum retry delay in milliseconds. The default value is " + GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT,
                GROUP_GCS_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG, ConfigDef.Type.DOUBLE,
                GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT, ConfigDef.Range.atLeast(1.0D), ConfigDef.Importance.MEDIUM,
                "Retry delay multiplier. The default value is " + GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT,
                GROUP_GCS_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG, ConfigDef.Type.INT,
                GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Retry max attempts. The default value is " + GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT,
                GROUP_GCS_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
        configDef.define(GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG, ConfigDef.Type.LONG,
                GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_DEFAULT, new ConfigDef.Validator() {

                    static final long TOTAL_TIME_MAX = 86_400_000; // 24 hours

                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (Objects.isNull(value)) {
                            return;
                        }
                        assert value instanceof Long;
                        final var longValue = (Long) value;
                        if (longValue < 0) {
                            throw new ConfigException(name, value, "Value must be at least 0");
                        } else if (longValue > TOTAL_TIME_MAX) {
                            throw new ConfigException(name, value,
                                    "Value must be no more than " + TOTAL_TIME_MAX + " (24 hours)");
                        }
                    }
                }, ConfigDef.Importance.MEDIUM,
                "Retry total timeout in milliseconds. The default value is "
                        + GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_DEFAULT,
                GROUP_GCS_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE, // NOPMD
                                                                                                 // retryPolicyGroupCounter
                                                                                                 // updated value never
                                                                                                 // used
                GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG);
    }

    private static void addFileConfigGroup(final ConfigDef configDef) {
        int fileGroupCounter = 0;
        configDef.define(FILE_NAME_PREFIX_CONFIG, ConfigDef.Type.STRING, "", new ConfigDef.Validator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                // See https://cloud.google.com/storage/docs/naming
                assert value instanceof String;
                final String valueStr = (String) value;
                if (valueStr.length() > 1024) { // NOPMD avoid literal
                    throw new ConfigException(GCS_BUCKET_NAME_CONFIG, value, "cannot be longer than 1024 characters");
                }
                if (valueStr.startsWith(".well-known/acme-challenge")) {
                    throw new ConfigException(GCS_BUCKET_NAME_CONFIG, value,
                            "cannot start with '.well-known/acme-challenge'");
                }
            }
        }, ConfigDef.Importance.MEDIUM, "The prefix to be added to the name of each file put on GCS.", GROUP_FILE,
                fileGroupCounter++, ConfigDef.Width.NONE, FILE_NAME_PREFIX_CONFIG);

        configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null,
                new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG), ConfigDef.Importance.MEDIUM,
                "The template for file names on GCS. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                        + "(the offset of the first record in the file). "
                        + "Only some combinations of variables are valid, which currently are:\n"
                        + "- `topic`, `partition`, `start_offset`.",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);

        final String supportedCompressionTypes = CompressionType.names()
                .stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, CompressionType.NONE.name,
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        assert value instanceof String;
                        final String valueStr = (String) value;
                        if (!CompressionType.names().contains(valueStr)) {
                            throw new ConfigException(FILE_COMPRESSION_TYPE_CONFIG, valueStr,
                                    "supported values are: " + supportedCompressionTypes);
                        }
                    }
                }, ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on GCS. " + "The supported values are: "
                        + supportedCompressionTypes + ".",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));

        configDef.define(FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, new ConfigDef.Validator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                assert value instanceof Integer;
                if ((Integer) value < 0) {
                    throw new ConfigException(FILE_MAX_RECORDS, value, "must be a non-negative integer number");
                }
            }
        }, ConfigDef.Importance.MEDIUM,
                "The maximum number of records to put in a single file. " + "Must be a non-negative integer number. "
                        + "0 is interpreted as \"unlimited\", which is the default.",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.SHORT, FILE_MAX_RECORDS);

        configDef.define(FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, ZoneOffset.UTC.toString(),
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        try {
                            ZoneId.of(value.toString());
                        } catch (final Exception e) { // NOPMD broad exception catched
                            throw new ConfigException(FILE_NAME_TIMESTAMP_TIMEZONE, value, e.getMessage());
                        }
                    }
                }, ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_TIMEZONE);

        configDef.define(FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING, TimestampSource.Type.WALLCLOCK.name(),
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        try {
                            TimestampSource.Type.of(value.toString());
                        } catch (final Exception e) { // NOPMD broad exception catched
                            throw new ConfigException(FILE_NAME_TIMESTAMP_SOURCE, value, e.getMessage());
                        }
                    }
                }, ConfigDef.Importance.LOW, "Specifies the the timestamp variable source. Default is wall-clock.",
                GROUP_FILE, fileGroupCounter, ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_SOURCE);

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
                LOG.warn(
                        "{{timestamp:unit=YYYY}} is no longer supported, "
                                + "please use {{timestamp:unit=yyyy}} instead. " + "It was automatically replaced: {}",
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
        final Boolean defaultCredentials = getBoolean(GCS_CREDENTIALS_DEFAULT_CONFIG);

        final long nonNulls = Stream.of(defaultCredentials, credentialsJson, credentialsPath)
                .filter(Objects::nonNull)
                .count();

        // only validate non nulls here, since all nulls means falling back to the default "no credential" behavour.
        if (nonNulls > MAX_ALLOWED_CREDENTIAL_CONFIGS) {
            throw new ConfigException(String.format("Only one of %s, %s, and %s can be non-null.",
                    GCS_CREDENTIALS_DEFAULT_CONFIG, GCS_CREDENTIALS_JSON_CONFIG, GCS_CREDENTIALS_PATH_CONFIG));
        }
    }

    public OAuth2Credentials getCredentials() {
        final String credentialsPath = getString(GCS_CREDENTIALS_PATH_CONFIG);
        final Password credentialsJsonPwd = getPassword(GCS_CREDENTIALS_JSON_CONFIG);
        final Boolean defaultCredentials = getBoolean(GCS_CREDENTIALS_DEFAULT_CONFIG);

        // if we've got no path, json and not configured to use default credentials, fall back to connecting without
        // any credentials at all.
        if (credentialsPath == null && credentialsJsonPwd == null
                && (defaultCredentials == null || !defaultCredentials)) {
            LOG.warn("No GCS credentials provided, trying to connect without credentials.");
            return NoCredentials.getInstance();
        }

        try {
            if (Boolean.TRUE.equals(defaultCredentials)) {
                return GoogleCredentials.getApplicationDefault();
            }

            String credentialsJson = null;
            if (credentialsJsonPwd != null) {
                credentialsJson = credentialsJsonPwd.value();
            }
            return GoogleCredentialsBuilder.build(credentialsPath, credentialsJson);
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConfigException("Failed to create GCS credentials: " + e.getMessage());
        }
    }

    public String getBucketName() {
        return getString(GCS_BUCKET_NAME_CONFIG);
    }

    @Override
    public CompressionType getCompressionType() {
        return CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG));
    }

    @Override
    public List<OutputField> getOutputFields() {
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

    public String getPrefix() {
        return getString(FILE_NAME_PREFIX_CONFIG);
    }

    public String getConnectorName() {
        return originalsStrings().get(NAME_CONFIG);
    }

    public int getGcsRetryBackoffMaxAttempts() {
        return getInt(GCS_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    public double getGcsRetryBackoffDelayMultiplier() {
        return getDouble(GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_CONFIG);
    }

    public Duration getGcsRetryBackoffTotalTimeout() {
        return Duration.ofMillis(getLong(GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_CONFIG));
    }

    public Duration getGcsRetryBackoffInitialDelay() {
        return Duration.ofMillis(getLong(GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG));
    }

    public Duration getGcsRetryBackoffMaxDelay() {
        return Duration.ofMillis(getLong(GCS_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG));
    }

    public String getGcsEndpoint() {
        return getString(GCS_ENDPOINT_CONFIG);
    }

    public String getUserAgent() {
        return getString(GCS_USER_AGENT);
    }
}
