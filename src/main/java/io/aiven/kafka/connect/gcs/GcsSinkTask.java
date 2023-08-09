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

import java.nio.channels.Channels;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.output.OutputWriter;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkConnector.class);
    private static final String USER_AGENT_HEADER_KEY = "user-agent";

    private RecordGrouper recordGrouper;

    private GcsSinkConfig config;

    private Storage storage;

    // required by Connect
    public GcsSinkTask() {
        super();
    }

    // for testing
    public GcsSinkTask(final Map<String, String> props, final Storage storage) {
        super();
        Objects.requireNonNull(props, "props cannot be null");
        Objects.requireNonNull(storage, "storage cannot be null");

        this.config = new GcsSinkConfig(props);
        this.storage = storage;
        initRest();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        this.config = new GcsSinkConfig(props);
        this.storage = StorageOptions.newBuilder()
                .setHost(config.getGcsEndpoint())
                .setCredentials(config.getCredentials())
                .setHeaderProvider(FixedHeaderProvider.create(USER_AGENT_HEADER_KEY, config.getUserAgent()))
                .setRetrySettings(RetrySettings.newBuilder()
                        .setInitialRetryDelay(config.getGcsRetryBackoffInitialDelay())
                        .setMaxRetryDelay(config.getGcsRetryBackoffMaxDelay())
                        .setRetryDelayMultiplier(config.getGcsRetryBackoffDelayMultiplier())
                        .setTotalTimeout(config.getGcsRetryBackoffTotalTimeout())
                        .setMaxAttempts(config.getGcsRetryBackoffMaxAttempts())
                        .build())
                .build()
                .getService();
        initRest();
        if (Objects.nonNull(config.getKafkaRetryBackoffMs())) {
            context.timeout(config.getKafkaRetryBackoffMs());
        }
    }

    private void initRest() {
        try {
            this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");

        LOG.debug("Processing {} records", records.size());
        for (final SinkRecord record : records) {
            recordGrouper.put(record);
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            recordGrouper.records().forEach(this::flushFile);
        } finally {
            recordGrouper.clear();
        }
    }

    private void flushFile(final String filename, final List<SinkRecord> records) {
        final BlobInfo blob = BlobInfo.newBuilder(config.getBucketName(), config.getPrefix() + filename).build();
        try (var out = Channels.newOutputStream(storage.writer(blob));
                var writer = OutputWriter.builder()
                        .withExternalProperties(config.originalsStrings())
                        .withOutputFields(config.getOutputFields())
                        .withCompressionType(config.getCompressionType())
                        .withEnvelopeEnabled(config.envelopeEnabled())
                        .build(out, config.getFormatType())) {
            writer.writeRecords(records);
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        // Nothing to do.
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
