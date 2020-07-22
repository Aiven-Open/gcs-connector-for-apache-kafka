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

package io.aiven.kafka.connect.gcs;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.Resources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GoogleCredentialsBuilderTest {
    private static final String BUCKET_PROPERTY = "integration-test.gcs.bucket";

    private static String testBucketName;

    @BeforeAll
    static void setUpAll() {
        testBucketName = System.getProperty(BUCKET_PROPERTY);
        Objects.requireNonNull(testBucketName, BUCKET_PROPERTY + " must be set");
    }

    @Test
    void testDefaultCredentials() throws IOException {
        final Storage storage = StorageOptions.newBuilder()
            .setCredentials(GoogleCredentialsBuilder.build(null, null))
            .build()
            .getService();
        assertNotNull(storage.get(testBucketName));
    }

    @Test
    void testCredentialsPathProvided() throws IOException {
        final String credentialsPath =
            getClass().getClassLoader().getResource("test_gcs_credentials.json").getPath();
        final GoogleCredentials credentials = GoogleCredentialsBuilder.build(credentialsPath, null);
        assertTrue(credentials instanceof UserCredentials);

        final UserCredentials userCredentials = (UserCredentials) credentials;
        assertEquals("test-client-id", userCredentials.getClientId());
        assertEquals("test-client-secret", userCredentials.getClientSecret());
    }

    @Test
    void testCredentialsJsonProvided() throws IOException {
        final String credentialsJson = Resources.toString(
            getClass().getClassLoader().getResource("test_gcs_credentials.json"),
            StandardCharsets.UTF_8);
        final GoogleCredentials credentials = GoogleCredentialsBuilder.build(null, credentialsJson);
        assertTrue(credentials instanceof UserCredentials);

        final UserCredentials userCredentials = (UserCredentials) credentials;
        assertEquals("test-client-id", userCredentials.getClientId());
        assertEquals("test-client-secret", userCredentials.getClientSecret());
    }

    @Test
    void testBothCredentialsPathAndCredentialsJsonProvided() throws IOException {
        final URL credentialResource = getClass().getClassLoader().getResource("test_gcs_credentials.json");
        final Throwable t = assertThrows(IllegalArgumentException.class, () ->
            GoogleCredentialsBuilder.build(
                credentialResource.getPath(),
                Resources.toString(credentialResource, StandardCharsets.UTF_8)));
        assertEquals("Both credentialsPath and credentialsJson cannot be non-null.", t.getMessage());
    }
}
