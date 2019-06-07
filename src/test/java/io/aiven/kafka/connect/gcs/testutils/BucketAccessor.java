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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

public final class BucketAccessor {
    private final Storage storage;
    private final String bucketName;
    private final boolean cache;

    private List<String> blobNamesCache = null;
    private final Map<String, String> stringContentCache = new HashMap<>();
    private final Map<String, List<String>> linesCache = new HashMap<>();
    private final Map<String, List<List<String>>> decodedLinesCache = new HashMap<>();

    public BucketAccessor(final Storage storage,
                          final String bucketName,
                          final boolean cache) {
        Objects.requireNonNull(storage);
        Objects.requireNonNull(bucketName);

        this.storage = storage;
        this.bucketName = bucketName;
        this.cache = cache;
    }

    public BucketAccessor(final Storage storage,
                          final String bucketName) {
        this(storage, bucketName, false);
    }

    public final void ensureWorking() {
        if (storage.get(bucketName) == null) {
            throw new RuntimeException("Cannot access GCS bucket \"" + bucketName + "\"");
        }
    }

    public final List<String> getBlobNames() {
        if (cache) {
            if (blobNamesCache == null) {
                blobNamesCache = getBlobNames0();
            }
            return blobNamesCache;
        } else {
            return getBlobNames0();
        }
    }

    private List<String> getBlobNames0() {
        return StreamSupport.stream(storage.list(bucketName).iterateAll().spliterator(), false)
                .map(BlobInfo::getName)
                .sorted()
                .collect(Collectors.toList());
    }

    public final void clear(final String prefix) {
        Objects.requireNonNull(prefix);

        final Storage.BlobListOption blobListOption = Storage.BlobListOption.prefix(prefix);
        for (final Blob blob : storage.get(bucketName).list(blobListOption).iterateAll()) {
            assert blob.delete();
        }

        if (cache) {
            blobNamesCache = null;
            stringContentCache.clear();
            linesCache.clear();
            decodedLinesCache.clear();
        }
    }

    public final String readStringContent(final String blobName,
                                          final boolean compressed) {
        Objects.requireNonNull(blobName);
        if (cache) {
            return stringContentCache.computeIfAbsent(
                    blobName, k -> readStringContent0(blobName, compressed));
        } else {
            return readStringContent0(blobName, compressed);
        }
    }

    private String readStringContent0(final String blobName,
                                      final boolean compressed) {
        if (compressed) {
            throw new IllegalArgumentException("compression is not implemented");
        }

        final byte[] blobBytes = storage.readAllBytes(bucketName, blobName);
        return new String(blobBytes);
    }

    public final List<String> readLines(final String blobName, final boolean compressed) {
        Objects.requireNonNull(blobName);
        if (cache) {
            return linesCache.computeIfAbsent(blobName, k -> readLines0(blobName, compressed));
        } else {
            return readLines0(blobName, compressed);
        }
    }

    private List<String> readLines0(final String blobName, final boolean compressed) {
        Objects.requireNonNull(blobName);
        final byte[] blobBytes = storage.readAllBytes(bucketName, blobName);
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
             final InputStream decompressedStream = getDecompressedStream(bais, compressed);
             final InputStreamReader reader = new InputStreamReader(decompressedStream, StandardCharsets.UTF_8);
             final BufferedReader bufferedReader = new BufferedReader(reader)) {

            return bufferedReader.lines().collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getDecompressedStream(final InputStream inputStream, final boolean compressed) throws IOException {
        Objects.requireNonNull(inputStream);

        if (compressed) {
            return new GZIPInputStream(inputStream);
        } else {
            return inputStream;
        }
    }

    public final List<List<String>> readAndDecodeLines(final String blobName,
                                                       final boolean compressed,
                                                       final int... fieldsToDecode) {
        Objects.requireNonNull(blobName);
        Objects.requireNonNull(fieldsToDecode);

        if (cache) {
            return decodedLinesCache.computeIfAbsent(
                    blobName, k -> readAndDecodeLines0(blobName, compressed, fieldsToDecode));
        } else {
            return readAndDecodeLines0(blobName, compressed, fieldsToDecode);
        }
    }

    private List<List<String>> readAndDecodeLines0(final String blobName,
                                                   final boolean compressed,
                                                   final int[] fieldsToDecode) {
        return readLines(blobName, compressed).stream()
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
