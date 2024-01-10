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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.gcs.GcsSinkConfig;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/**
 * Tests {@link GcsSinkConfig} class.
 */
final class GcsSinkCredentialsConfigTest {
    @ParameterizedTest
    @ValueSource(strings = { "", "{{topic}}", "{{partition}}", "{{start_offset}}", "{{topic}}-{{partition}}",
            "{{topic}}-{{start_offset}}", "{{partition}}-{{start_offset}}",
            "{{topic}}-{{partition}}-{{start_offset}}-{{unknown}}" })
    void incorrectFilenameTemplates(final String template) {
        final Map<String, String> properties = Map.of("file.name.template", template, "gcs.bucket.name", "some-bucket");

        final ConfigValue configValue = GcsSinkConfig.configDef()
                .validate(properties)
                .stream()
                .filter(x -> GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG.equals(x.name()))
                .findFirst()
                .get();
        assertFalse(configValue.errorMessages().isEmpty());

        final var throwable = assertThrows(ConfigException.class, () -> new GcsSinkConfig(properties));
        assertTrue(throwable.getMessage().startsWith("Invalid value "));
    }

    @Test
    void gcsCredentialsPath() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "gcs.credentials.path",
                Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json").getPath());

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
                Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json"),
                StandardCharsets.UTF_8);
        properties.put("gcs.credentials.json", credentialsJson);

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final UserCredentials credentials = (UserCredentials) config.getCredentials();
        assertEquals("test-client-id", credentials.getClientId());
        assertEquals("test-client-secret", credentials.getClientSecret());
    }

    /**
     * This test validates that the NoCredentials are used when default is specified as false. This behaviour mimics
     * that of the Tiered Storage Manager.
     */
    @Test
    void gcsCredentialsNoCredentialsWhenDefaultCredentialsFalse() {
        final Map<String, String> properties = Map.of(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, "test-bucket",
                GcsSinkConfig.GCS_CREDENTIALS_DEFAULT_CONFIG, String.valueOf(false));

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);

        final Credentials credentials = config.getCredentials();
        assertEquals(NoCredentials.getInstance(), credentials);
    }

    /** Verifies that NoCredentials are used when no credential configurations is supplied. */
    @Test
    void gcsCredentialsNoCredentialsWhenNoCredentialsSupplied() {
        final Map<String, String> properties = Map.of(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, "test-bucket");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);

        final Credentials credentials = config.getCredentials();
        assertEquals(NoCredentials.getInstance(), credentials);
    }

    @Test
    void gcsCredentialsDefault() {
        final Map<String, String> properties = Map.of(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, "test-bucket",
                GcsSinkConfig.GCS_CREDENTIALS_DEFAULT_CONFIG, String.valueOf(true));

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);

        // Note that we're using a mock here since the Google credentials are not part of the environment when running
        // in github actions. It's better to use a mock here and make the test self-contained than it is to make things
        // more complicated and making it rely on the environment it's executing within.
        try (MockedStatic<GoogleCredentials> mocked = mockStatic(GoogleCredentials.class)) {
            final GoogleCredentials googleCredentials = mock(GoogleCredentials.class);
            mocked.when(GoogleCredentials::getApplicationDefault).thenReturn(googleCredentials);

            final OAuth2Credentials credentials = config.getCredentials();
            assertEquals(googleCredentials, credentials);
        }
    }

    @ParameterizedTest
    @MethodSource("provideMoreThanOneNonNull")
    void gcsCredentialsExclusivity(final Boolean defaultCredentials, final String credentialsJson,
            final String credentialsPath) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, "test-bucket");
        properties.put(GcsSinkConfig.GCS_CREDENTIALS_DEFAULT_CONFIG,
                defaultCredentials == null ? null : String.valueOf(defaultCredentials));
        properties.put(GcsSinkConfig.GCS_CREDENTIALS_JSON_CONFIG, credentialsJson);
        properties.put(GcsSinkConfig.GCS_CREDENTIALS_PATH_CONFIG, credentialsPath);

        // Should pass here, because ConfigDef validation doesn't check interdependencies.
        assertConfigDefValidationPasses(properties);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new GcsSinkConfig(properties));
        assertEquals(
                "Only one of gcs.credentials.default, gcs.credentials.json, and gcs.credentials.path can be non-null.",
                throwable.getMessage());
    }

    private static Stream<Arguments> provideMoreThanOneNonNull() {
        return Stream.of(Arguments.of(true, "json", "path"), Arguments.of(false, "json", "path"),
                Arguments.of(true, "json", null), Arguments.of(false, "json", null), Arguments.of(true, null, "path"),
                Arguments.of(false, null, "path"), Arguments.of(null, "json", "path"));
    }

    private void assertConfigDefValidationPasses(final Map<String, String> properties) {
        for (final ConfigValue configValue : GcsSinkConfig.configDef().validate(properties)) {
            assertTrue(configValue.errorMessages().isEmpty());
        }
    }
}
