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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.auth.oauth2.GoogleCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GoogleCredentialsBuilder {
    private static final Logger log = LoggerFactory.getLogger(GoogleCredentialsBuilder.class);

    /**
     * Builds {@link GoogleCredentials} using the provided credentials path and credentials JSON.
     *
     * <p>{@code credentialsPath} and {@code credentialsJson} are mutually exclusive.
     * So if both are provided (are non-{@code null}), this is an error.
     *
     * <p>If either @code credentialsPath} or {@code credentialsJson} is provided,
     * it's used to construct the credentials.
     *
     * <p>If none are provided, the default GCP SDK credentials acquisition mechanism is used.
     *
     * @param credentialsPath the credential path, can be {@code null}.
     * @param credentialsJson the credential JSON string, can be {@code null}.
     * @return a {@link GoogleCredentials} constructed based on the input.
     * @throws IOException              if some error getting the credentials happen.
     * @throws IllegalArgumentException
     * if both {@code credentialsPath} and {@code credentialsJson} are non-{@code null}.
     */
    public static GoogleCredentials build(final String credentialsPath,
                                          final String credentialsJson) throws IOException, IllegalArgumentException {
        if (credentialsPath != null && credentialsJson != null) {
            throw new IllegalArgumentException("Both credentialsPath and credentialsJson cannot be non-null.");
        }

        if (credentialsPath != null) {
            log.debug("Using provided credentials path");
            return getCredentialsFromPath(credentialsPath);
        }

        if (credentialsJson != null) {
            log.debug("Using provided credentials JSON");
            return getCredentialsFromJson(credentialsJson);
        }

        log.debug("Using default credentials");
        return GoogleCredentials.getApplicationDefault();
    }

    private static GoogleCredentials getCredentialsFromPath(final String credentialsPath) throws IOException {
        try (final InputStream stream = new FileInputStream(credentialsPath)) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            throw new IOException("Failed to read GCS credentials from " + credentialsPath, e);
        }
    }

    private static GoogleCredentials getCredentialsFromJson(final String credentialsJson) throws IOException {
        try (final InputStream stream = new ByteArrayInputStream(credentialsJson.getBytes())) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            throw new IOException("Failed to read credentials from JSON string", e);
        }
    }
}
