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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.gcs.config.CompressionType;
import io.aiven.kafka.connect.gcs.config.GcsSinkConfig;
import io.aiven.kafka.connect.gcs.output.OutputWriter;
import io.aiven.kafka.connect.gcs.templating.Template;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(GcsSinkConnector.class);

    private RecordGrouper recordGrouper;

    private OutputWriter outputWriter;

    private GcsSinkConfig config;
    private Storage storage;

    // required by Connect
    public GcsSinkTask() {
    }

    // for testing
    protected GcsSinkTask(final Map<String, String> props,
                          final Storage storage) {
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
            .setCredentials(config.getCredentials())
            .build()
            .getService();
        initRest();
    }

    private void initRest() {
        this.outputWriter = OutputWriter.builder().addFields(config.getOutputFields()).build();

        final Integer maxRecordsPerFile;
        if (config.getMaxRecordsPerFile() == 0) {
            maxRecordsPerFile = null;
        } else {
            maxRecordsPerFile = config.getMaxRecordsPerFile();
        }

        final Template filenameTemplate = config.getFilenameTemplate();
        if (TopicPartitionRecordGrouper.acceptsTemplate(filenameTemplate)) {
            this.recordGrouper = new TopicPartitionRecordGrouper(filenameTemplate, maxRecordsPerFile);
        } else if (KeyRecordGrouper.acceptsTemplate(filenameTemplate)) {
            if (maxRecordsPerFile == null) {
                log.info("File name template is {}, 1 record per file is used", filenameTemplate);
            }
            this.recordGrouper = new KeyRecordGrouper(filenameTemplate);
        } else {
            throw new ConnectException("Unsupported file name template " + filenameTemplate);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");

        log.debug("Processing {} records", records.size());
        for (final SinkRecord record : records) {
            recordGrouper.put(record);
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        for (final Map.Entry<String, List<SinkRecord>> entry : recordGrouper.records().entrySet()) {
            flushFile(entry.getKey(), entry.getValue());
        }
        recordGrouper.clear();
    }

    private void flushFile(final String filename, final List<SinkRecord> records) {
        final BlobInfo blob = BlobInfo
            .newBuilder(config.getBucketName(), config.getPrefix() + filename)
            .build();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // Don't group these two tries,
            // because the internal one must be closed before writing to GCS.
            try (final OutputStream compressedStream = getCompressedStream(baos)) {
                for (int i = 0; i < records.size() - 1; i++) {
                    outputWriter.writeRecord(records.get(i), compressedStream);
                }
                outputWriter.writeLastRecord(records.get(records.size() - 1), compressedStream);
            }
            storage.create(blob, baos.toByteArray());
        } catch (final Exception e) {
            throw new ConnectException(e);
        }
    }

    private OutputStream getCompressedStream(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream, "outputStream cannot be null");

        if (config.getCompressionType() == CompressionType.GZIP) {
            return new GZIPOutputStream(outputStream);
        } else {
            return outputStream;
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
