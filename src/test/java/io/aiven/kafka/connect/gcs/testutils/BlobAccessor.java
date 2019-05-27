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

package io.aiven.kafka.connect.gcs.testutils;

import com.google.cloud.storage.Storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public final class BlobAccessor {
    private final Storage storage;
    private final String bucketName;
    private final String blobName;
    private final boolean compressed;

    public BlobAccessor(final Storage storage,
                        final String bucketName,
                        final String blobName,
                        final boolean compressed) {
        Objects.requireNonNull(storage);
        Objects.requireNonNull(bucketName);
        Objects.requireNonNull(blobName);

        this.storage = storage;
        this.bucketName = bucketName;
        this.blobName = blobName;
        this.compressed = compressed;
    }

    public final String readStringContent() throws IOException {
        final byte[] blobBytes = storage.readAllBytes(bucketName, blobName);
        return new String(blobBytes);
    }

    public final List<String> readLines() throws IOException {
        final byte[] blobBytes = storage.readAllBytes(bucketName, blobName);
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
             final InputStream decompressedStream = getDecompressedStream(bais);
             final InputStreamReader reader = new InputStreamReader(decompressedStream, StandardCharsets.UTF_8);
             final BufferedReader bufferedReader = new BufferedReader(reader)) {

            return bufferedReader.lines().collect(Collectors.toList());
        }
    }

    private InputStream getDecompressedStream(final InputStream inputStream) throws IOException {
        Objects.requireNonNull(inputStream);

        if (compressed) {
            return new GZIPInputStream(inputStream);
        } else {
            return inputStream;
        }
    }

    public final List<List<String>> readAndDecodeLines(final int... fieldsToDecode) throws IOException {
        Objects.requireNonNull(fieldsToDecode);

        return readLines().stream()
                .map(l -> l.split(","))
                .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
                .collect(Collectors.toList());
    }

    private List<String> decodeRequiredFields(final String[] originalFields, final int[] fieldsToDecode) {
        Objects.requireNonNull(originalFields);
        Objects.requireNonNull(fieldsToDecode);

        final List<String> result = Arrays.asList(originalFields);
        for (final int fieldIdx : fieldsToDecode) {
            result.set(fieldIdx, b64Decode(result.get(fieldIdx)));
        }
        return result;
    }

    private String b64Decode(final String value) {
        Objects.requireNonNull(value);

        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
