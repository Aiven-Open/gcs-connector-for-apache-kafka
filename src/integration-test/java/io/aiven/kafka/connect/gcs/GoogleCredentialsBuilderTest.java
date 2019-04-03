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

package io.aiven.kafka.connect.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.aiven.kafka.connect.gcs.gcs.GoogleCredentialsBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

final class GoogleCredentialsBuilderTest {
    private static String testBucketName;

    @BeforeAll
    static void setUpAll() {
        testBucketName = System.getProperty("integration-test.gcs.bucket");
    }

    @Test
    void testNoCredentialsPathProvided() throws IOException {
        final Storage storage = StorageOptions.newBuilder()
                .setCredentials(GoogleCredentialsBuilder.build(null))
                .build()
                .getService();
        assertNotNull(storage.get(testBucketName));
    }

    @Test
    void testCredentialsPathProvided() throws IOException {
        final String credentialsPath =
                getClass().getClassLoader().getResource("test_gcs_credentials.json").getPath();
        final GoogleCredentials credentials = GoogleCredentialsBuilder.build(credentialsPath);
        assertTrue(credentials instanceof UserCredentials);

        final UserCredentials userCredentials = (UserCredentials) credentials;
        assertAll(
                () -> assertEquals("test-client-id", userCredentials.getClientId()),
                () -> assertEquals("test-client-secret", userCredentials.getClientSecret())
        );
    }
}
