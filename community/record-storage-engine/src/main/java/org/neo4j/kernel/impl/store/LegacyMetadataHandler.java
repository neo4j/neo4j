/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import static java.lang.String.format;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.io.pagecache.IOController.DISABLED;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.allFormats;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.RECORD_SIZE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.util.BitBuffer;

/**
 * The content layout of metadata store changed in 5.0
 * and current {@link MetaDataStore} cannot read and interpret the content of pre 5.0 metadata stores,
 * so this class also contains code that can do that.
 */
public class LegacyMetadataHandler {

    private static final String UNKNOWN_VERSION = "Unknown";
    private static final UUID NOT_INITIALIZED_UUID = new UUID(Long.MIN_VALUE, Long.MIN_VALUE);
    // Mapping of known legacy format version identifiers
    private static final Map<String, StoreVersion> LEGACY_VERSION_MAPPING = Map.of(
            "SF4.3.0", StoreVersion.STANDARD_V4_3,
            "AF4.3.0", StoreVersion.ALIGNED_V4_3,
            "HL4.3.0", StoreVersion.HIGH_LIMIT_V4_3);

    public static Metadata44 readMetadata44FromStore(
            PageCache pageCache, Path metadataStore, String databaseName, CursorContext cursorContext)
            throws IOException {
        try (PagedFile pagedFile = pageCache.map(
                metadataStore,
                pageCache.pageSize(),
                databaseName,
                immutable.of(PageCacheOpenOptions.BIG_ENDIAN),
                DISABLED)) {
            if (pagedFile.getLastPageId() < 0) {
                throw new IllegalStateException("Metadata store is empty");
            }

            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                if (!cursor.next()) {
                    throw new IllegalStateException("Metadata store is empty");
                }

                RuntimeException exception;
                Metadata44 result = null;
                do {
                    exception = null;
                    try {
                        KernelVersion kernelVersion;
                        try {
                            long kernelVersionBits = readLongRecord(19, cursor);
                            kernelVersion = KernelVersion.getForVersion(Numbers.safeCastLongToByte(kernelVersionBits));
                        } catch (NotInUseException e) {
                            kernelVersion = KernelVersion.EARLIEST;
                        }

                        UUID externalId = readUUID(16, cursor);
                        UUID databaseId = readUUID(20, cursor);

                        try {
                            var storeId = storeIdFromLegacyMetadata(
                                    readLongRecord(0, cursor), readLongRecord(1, cursor), readLongRecord(4, cursor));
                            result = new Metadata44(storeId, externalId, kernelVersion, databaseId);
                        } catch (NotInUseException e) {
                            // Store ID is expected to be always present
                            // if it is not, it means that there is something fishy about the store
                            exception = new IllegalStateException("Failed to load data from legacy metadata store");
                        }
                    } catch (RuntimeException e) {
                        exception = e;
                    }
                } while (cursor.shouldRetry());
                if (exception != null) {
                    throw exception;
                }

                return result;
            }
        }
    }

    private static UUID readUUID(int firstId, PageCursor cursor) {
        try {
            var uuid = new UUID(readLongRecord(firstId, cursor), readLongRecord(firstId + 1, cursor));
            // Unfortunately, uninitialised UUID fields come in two flavours.
            // The record can be either unused as expected with records in this storage engine
            // or it can set to a special UUID constant
            if (uuid.equals(NOT_INITIALIZED_UUID)) {
                return null;
            }
            return uuid;
        } catch (NotInUseException e) {
            return null;
        }
    }

    private static long readLongRecord(int id, PageCursor cursor) throws NotInUseException {
        cursor.setOffset(id * RECORD_SIZE);
        if (cursor.getByte() != Record.IN_USE.byteValue()) {
            throw new NotInUseException();
        }
        return cursor.getLong();
    }

    private static StoreId storeIdFromLegacyMetadata(long creationTime, long random, long legacyVersion) {
        String versionString = versionLongToString(legacyVersion);
        var version = LEGACY_VERSION_MAPPING.get(versionString);
        if (version == null) {
            throw new IllegalArgumentException(
                    "Unable to read store with version '" + versionString + "'. "
                            + "Please make sure that database is migrated properly to be supported by current version of neo4j.");
        }
        RecordFormats recordFormat = Iterables.stream(allFormats())
                .filter(format -> format.getFormatFamily()
                                .name()
                                .equals(version.formatFamily().name())
                        && format.majorVersion() == version.majorVersion()
                        && format.minorVersion() == version.minorVersion())
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Unknown store version '" + versionString + "'"));

        return new StoreId(
                creationTime,
                random,
                RecordStorageEngineFactory.NAME,
                recordFormat.getFormatFamily().name(),
                recordFormat.majorVersion(),
                recordFormat.minorVersion(),
                versionString);
    }

    private static String versionLongToString(long storeVersion) {
        if (storeVersion == -1) {
            return UNKNOWN_VERSION;
        }
        BitBuffer bits = BitBuffer.bitsFromLongs(new long[] {storeVersion});
        int length = bits.getShort(8);
        if (length == 0 || length > 7) {
            throw new IllegalArgumentException(format("The read version string length %d is not proper.", length));
        }
        char[] result = new char[length];
        for (int i = 0; i < length; i++) {
            result[i] = (char) bits.getShort(8);
        }
        return new String(result);
    }

    public record Metadata44(
            StoreId storeId, UUID maybeExternalId, KernelVersion kernelVersion, UUID maybeDatabaseId) {}

    private static class NotInUseException extends Exception {}
}
