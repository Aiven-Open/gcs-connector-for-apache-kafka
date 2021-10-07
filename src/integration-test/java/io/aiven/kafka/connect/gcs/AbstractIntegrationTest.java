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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.BeforeAll;

abstract class AbstractIntegrationTest {
    protected static final String TEST_TOPIC_0 = "test-topic-0";
    protected static final String TEST_TOPIC_1 = "test-topic-1";

    protected static String gcsCredentialsPath;
    protected static String gcsCredentialsJson;

    protected static String testBucketName;

    protected static String gcsPrefix;

    protected static BucketAccessor testBucketAccessor;

    protected static File pluginDir;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        gcsCredentialsPath = System.getProperty("integration-test.gcs.credentials.path");
        gcsCredentialsJson = System.getProperty("integration-test.gcs.credentials.json");

        testBucketName = System.getProperty("integration-test.gcs.bucket");

        final Storage storage = StorageOptions.newBuilder()
            .setCredentials(GoogleCredentialsBuilder.build(gcsCredentialsPath, gcsCredentialsJson))
            .build()
            .getService();
        testBucketAccessor = new BucketAccessor(storage, testBucketName);
        testBucketAccessor.ensureWorking();

        gcsPrefix = "gcs-connector-for-apache-kafka-test-"
            + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final File testDir = Files.createTempDirectory("gcs-connector-for-apache-kafka-test-").toFile();

        pluginDir = new File(testDir, "plugins/gcs-connector-for-apache-kafka/");
        assert pluginDir.mkdirs();

        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
            distFile.toString(), pluginDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    protected String getBlobName(final int partition, final int startOffset, final String compression) {
        final String result = String.format("%s%s-%d-%d", gcsPrefix, TEST_TOPIC_0, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    protected String getBlobName(final String key, final String compression) {
        final String result = String.format("%s%s", gcsPrefix, key);
        return result + CompressionType.forName(compression).extension();
    }
}
