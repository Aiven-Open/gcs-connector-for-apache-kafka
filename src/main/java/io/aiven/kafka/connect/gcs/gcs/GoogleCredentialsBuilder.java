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

package io.aiven.kafka.connect.gcs.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class GoogleCredentialsBuilder {
    private static final Logger log = LoggerFactory.getLogger(GoogleCredentialsBuilder.class);

    /**
     * Builds {@link GoogleCredentials} using the provided credentials path and credentials JSON.
     *
     * <p>{@code credentialsPath} and {@code credentialsJson} are mutually exclusive.
     * So if both are provided (are non-{@code null}), this is an error.
     *
     * <p>If either @code credentialsPath} or {@code credentialsJson} is provided, it's used to construct the credentials.
     *
     * <p>If none are provided, the default GCP SDK credentials acquisition mechanism is used.
     *
     * @param credentialsPath the credential path, can be {@code null}.
     * @param credentialsJson the credential JSON string, can be {@code null}.
     * @return a {@link GoogleCredentials} constructed based on the input.
     * @throws IOException if some error getting the credentials happen.
     * @throws IllegalArgumentException if both {@code credentialsPath} and {@code credentialsJson} are non-{@code null}.
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
