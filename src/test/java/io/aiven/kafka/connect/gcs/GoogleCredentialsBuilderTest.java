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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

final class GoogleCredentialsBuilderTest {

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Test
    void testDefaultCredentials() throws IOException {
        try (MockedStatic<GoogleCredentials> mocked = mockStatic(GoogleCredentials.class)) {
            final GoogleCredentials googleCredentials = mock(GoogleCredentials.class);
            mocked.when(GoogleCredentials::getApplicationDefault).thenReturn(googleCredentials);
            assertSame(GoogleCredentialsBuilder.build(null, null), googleCredentials);
            mocked.verify(GoogleCredentials::getApplicationDefault);
        }
    }

    @Test
    void testCredentialsPathProvided() throws IOException {
        final String credentialsPath = Thread.currentThread()
                .getContextClassLoader()
                .getResource("test_gcs_credentials.json")
                .getPath();
        final OAuth2Credentials credentials = GoogleCredentialsBuilder.build(credentialsPath, null);
        assertTrue(credentials instanceof UserCredentials);

        final UserCredentials userCredentials = (UserCredentials) credentials;
        assertEquals("test-client-id", userCredentials.getClientId());
        assertEquals("test-client-secret", userCredentials.getClientSecret());
    }

    @Test
    void testCredentialsJsonProvided() throws IOException {
        final String credentialsJson = Resources.toString(
                Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json"),
                StandardCharsets.UTF_8);
        final OAuth2Credentials credentials = GoogleCredentialsBuilder.build(null, credentialsJson);
        assertTrue(credentials instanceof UserCredentials);

        final UserCredentials userCredentials = (UserCredentials) credentials;
        assertEquals("test-client-id", userCredentials.getClientId());
        assertEquals("test-client-secret", userCredentials.getClientSecret());
    }

    @Test
    void testBothCredentialsPathAndCredentialsJsonProvided() {
        final URL credentialResource = Thread.currentThread()
                .getContextClassLoader()
                .getResource("test_gcs_credentials.json");
        final Throwable throwable = assertThrows(IllegalArgumentException.class, () -> GoogleCredentialsBuilder
                .build(credentialResource.getPath(), Resources.toString(credentialResource, StandardCharsets.UTF_8)));
        assertEquals("Both credentialsPath and credentialsJson cannot be non-null.", throwable.getMessage());
    }
}
