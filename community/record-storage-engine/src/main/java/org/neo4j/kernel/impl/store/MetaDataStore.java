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

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.internal.id.EmptyIdGeneratorFactory.EMPTY_ID_GENERATOR_FACTORY;
import static org.neo4j.io.pagecache.IOController.DISABLED;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.RECORD_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.StoreFileClosedException;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.util.HighestTransactionId;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence.Meta;

public class MetaDataStore extends CommonAbstractStore<MetaDataRecord, NoStoreHeader> implements MetadataProvider {
    private static final String TYPE_DESCRIPTOR = "NeoStore";
    // Stores created post 5.0 and migrated 4.4 stores must have LEGACY_STORE_VERSION position set to this value
    // if you ever wonder what this is, it is just a random 8 byte prime number
    private static final long LEGACY_STORE_VERSION_VALUE = 0xcf1bbcdcb7a56463L;

    // MetaDataStore always big-endian and never multi-versioned, so we can read store version regardless of endianness
    // of other stores
    private static final ImmutableSet<OpenOption> REQUIRED_OPTIONS = immutable.of(PageCacheOpenOptions.BIG_ENDIAN);
    private static final ImmutableSet<OpenOption> FORBIDDEN_OPTIONS =
            immutable.of(PageCacheOpenOptions.MULTI_VERSIONED);

    // Positions of meta-data records
    // Metadata store is split into fixed 8 byte slots.
    // Most values stored in the store take more than one slot.
    // Position is information about which slots are occupied by which value.
    private enum Position {
        EXTERNAL_STORE_UUID(
                0,
                2,
                "Database identifier exposed as external store identity. Generated on creation and never updated"),
        DATABASE_ID(2, 2, "The last used DatabaseId for this database"),
        LEGACY_STORE_VERSION(
                4,
                1,
                "Legacy store format version. This field is used from 5.0 onwards only to distinguish non-migrated pre 5.0 metadata stores."),
        STORE_ID(5, 8, "Store ID");

        private final int firstSlotId;
        private final int slotCount;
        private final String description;

        Position(int firstSlotId, int slotCount, String description) {
            this.firstSlotId = firstSlotId;
            this.slotCount = slotCount;
            this.description = description;
        }
    }

    private volatile StoreId storeId;
    private volatile long legacyStoreVersion = LEGACY_STORE_VERSION_VALUE;
    private volatile UUID externalStoreUUID;
    private volatile UUID databaseUUID;

    private final AtomicLong logVersion;
    private final AtomicLong checkpointLogVersion;
    private final AtomicLong lastCommittingTx;
    private final Supplier<StoreId> storeIdFactory;

    private final HighestTransactionId highestCommittedTransaction;

    private final OutOfOrderSequence lastClosedTx;
    private volatile boolean closed;
    private final AtomicLong appendIndex;
    private volatile AppendBatchInfo lastBatch;

    MetaDataStore(
            FileSystemAbstraction fileSystem,
            Path file,
            Config conf,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            RecordFormat<MetaDataRecord> recordFormat,
            boolean readOnly,
            LogTailLogVersionsMetadata logTailMetadata,
            String databaseName,
            ImmutableSet<OpenOption> openOptions,
            Supplier<StoreId> storeIdFactory) {
        super(
                fileSystem,
                file,
                null,
                conf,
                null,
                EMPTY_ID_GENERATOR_FACTORY,
                pageCache,
                pageCacheTracer,
                logProvider,
                TYPE_DESCRIPTOR,
                recordFormat,
                NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT,
                readOnly,
                databaseName,
                buildOptions(openOptions));

        checkpointLogVersion = new AtomicLong(logTailMetadata.getCheckpointLogVersion());
        logVersion = new AtomicLong(logTailMetadata.getLogVersion());
        this.storeIdFactory = storeIdFactory;
        var lastCommittedTx = logTailMetadata.getLastCommittedTransaction();
        lastCommittingTx = new AtomicLong(lastCommittedTx.id());
        highestCommittedTransaction = new HighestTransactionId(lastCommittedTx);
        var logPosition = logTailMetadata.getLastTransactionLogPosition();
        long lastCommittedAppendIndex = logTailMetadata.getLastCheckpointedAppendIndex();
        appendIndex = new AtomicLong(lastCommittedAppendIndex);
        lastBatch = new AppendBatchInfo(lastCommittedAppendIndex, LogPosition.UNSPECIFIED);
        lastClosedTx = new ArrayQueueOutOfOrderSequence(
                lastCommittedTx.id(),
                200,
                new Meta(
                        logPosition.getLogVersion(),
                        logPosition.getByteOffset(),
                        lastCommittedTx.kernelVersion().version(),
                        lastCommittedTx.checksum(),
                        lastCommittedTx.commitTimestamp(),
                        lastCommittedTx.consensusIndex(),
                        lastCommittedTx.appendIndex()));
    }

    private static ImmutableSet<OpenOption> buildOptions(ImmutableSet<OpenOption> openOptions) {
        return openOptions.newWithoutAll(FORBIDDEN_OPTIONS).newWithAll(REQUIRED_OPTIONS);
    }

    @Override
    public long nextAppendIndex() {
        return appendIndex.incrementAndGet();
    }

    @Override
    public long getLastAppendIndex() {
        return appendIndex.get();
    }

    @Override
    protected void initialiseNewStoreFile(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        super.initialiseNewStoreFile(flushEvent, cursorContext);
        StoreId storeId = storeIdFactory.get();
        generateMetadataFile(storeId, UUID.randomUUID(), null, cursorContext);
    }

    @Override
    protected void initialise(CursorContextFactory contextFactory) {
        super.initialise(contextFactory);
        try (CursorContext context = contextFactory.create("readMetadata")) {
            readMetadataFile(context);
        }
    }

    public long getHighId() {
        Position[] values = Position.values();
        Position lastPosition = values[values.length - 1];
        return lastPosition.firstSlotId + lastPosition.slotCount + 1;
    }

    @Override
    public long getCheckpointLogVersion() {
        assertNotClosed();
        return checkpointLogVersion.get();
    }

    @Override
    public void setCheckpointLogVersion(long version) {
        checkpointLogVersion.set(version);
    }

    @Override
    public long incrementAndGetCheckpointLogVersion() {
        return checkpointLogVersion.incrementAndGet();
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion,
            long appendIndex) {
        assertNotClosed();
        lastCommittingTx.set(transactionId);
        lastClosedTx.set(
                transactionId,
                new Meta(
                        logVersion,
                        byteOffset,
                        kernelVersion.version(),
                        checksum,
                        commitTimestamp,
                        consensusIndex,
                        transactionAppendIndex));
        highestCommittedTransaction.set(
                transactionId, transactionAppendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
        this.appendIndex.set(appendIndex);
        this.lastBatch = new AppendBatchInfo(appendIndex, LogPosition.UNSPECIFIED);
    }

    @Override
    public void regenerateMetadata(StoreId storeId, UUID externalStoreUUID, CursorContext cursorContext) {
        generateMetadataFile(storeId, externalStoreUUID, null, cursorContext);
        readMetadataFile(cursorContext);
    }

    @Override
    public void setDatabaseIdUuid(UUID uuid, CursorContext cursorContext) {
        assertNotClosed();
        generateMetadataFile(getStoreId(), externalStoreUUID, uuid, cursorContext);
        readMetadataFile(cursorContext);
    }

    @Override
    public Optional<UUID> getDatabaseIdUuid(CursorContext cursorContext) {
        assertNotClosed();
        var databaseUUID = this.databaseUUID;
        return Optional.ofNullable(databaseUUID);
    }

    @Override
    public StoreId getStoreId() {
        assertNotClosed();
        return storeId;
    }

    @Override
    public ExternalStoreId getExternalStoreId() {
        assertNotClosed();
        return new ExternalStoreId(externalStoreUUID);
    }

    @Override
    public long getCurrentLogVersion() {
        assertNotClosed();
        return logVersion.get();
    }

    @Override
    public void setCurrentLogVersion(long version) {
        logVersion.set(version);
    }

    @Override
    public long incrementAndGetVersion() {
        return logVersion.incrementAndGet();
    }

    @Override
    public long nextCommittingTransactionId() {
        assertNotClosed();
        return lastCommittingTx.incrementAndGet();
    }

    @Override
    public long committingTransactionId() {
        assertNotClosed();
        return lastCommittingTx.get();
    }

    @Override
    public void transactionCommitted(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        assertNotClosed();
        highestCommittedTransaction.offer(
                transactionId, appendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
    }

    @Override
    public long getLastCommittedTransactionId() {
        assertNotClosed();
        return highestCommittedTransaction.get().id();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        assertNotClosed();
        return highestCommittedTransaction.get();
    }

    @Override
    public long getLastClosedTransactionId() {
        assertNotClosed();
        return lastClosedTx.getHighestGapFreeNumber();
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        assertNotClosed();
        return new TransactionIdSnapshot(lastClosedTx.reverseSnapshot());
    }

    @Override
    public ClosedTransactionMetadata getLastClosedTransaction() {
        assertNotClosed();
        return new ClosedTransactionMetadata(lastClosedTx.get());
    }

    @Override
    public void transactionClosed(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        lastClosedTx.offer(
                transactionId,
                new Meta(
                        logVersion,
                        byteOffset,
                        kernelVersion.version(),
                        checksum,
                        commitTimestamp,
                        consensusIndex,
                        appendIndex));
    }

    @Override
    public void resetLastClosedTransaction(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        assertNotClosed();
        lastClosedTx.set(
                transactionId,
                new Meta(
                        logVersion,
                        byteOffset,
                        kernelVersion.version(),
                        checksum,
                        commitTimestamp,
                        consensusIndex,
                        appendIndex));
    }

    @Override
    public void appendBatch(long appendIndex, LogPosition logPositionAfter) {
        lastBatch = new AppendBatchInfo(appendIndex, logPositionAfter);
    }

    @Override
    public AppendBatchInfo lastBatch() {
        return lastBatch;
    }

    public void logRecords(final DiagnosticsLogger logger) {
        for (Position position : Position.values()) {
            var value =
                    switch (position) {
                        case STORE_ID -> storeId;
                        case EXTERNAL_STORE_UUID -> externalStoreUUID;
                        case DATABASE_ID -> databaseUUID;
                        case LEGACY_STORE_VERSION -> Long.toString(legacyStoreVersion);
                    };

            logger.log(position.name() + " (" + position.description + "): " + value);
        }
    }

    @Override
    public MetaDataRecord newRecord() {
        return new MetaDataRecord();
    }

    @Override
    public void prepareForCommit(
            MetaDataRecord record,
            IdSequence idSequence,
            CursorContext cursorContext) { // No need to do anything with these records before commit
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            closed = true;
        }
    }

    private void assertNotClosed() {
        if (closed) {
            throw new StoreFileClosedException(storageFile);
        }
    }

    private void generateMetadataFile(
            StoreId storeId, UUID externalStoreUUID, UUID databaseUUID, CursorContext cursorContext) {
        try (var cursor = openPageCursorForWriting(0, cursorContext)) {
            if (cursor.next()) {
                writeLongRecord(cursor, externalStoreUUID.getMostSignificantBits());
                writeLongRecord(cursor, externalStoreUUID.getLeastSignificantBits());
                if (databaseUUID != null) {
                    writeLongRecord(cursor, databaseUUID.getMostSignificantBits());
                    writeLongRecord(cursor, databaseUUID.getLeastSignificantBits());
                } else {
                    writeEmptyRecord(cursor);
                    writeEmptyRecord(cursor);
                }
                writeLongRecord(cursor, LEGACY_STORE_VERSION_VALUE);

                writeStoreId(cursor, storeId);

            } else {
                throw new IllegalStateException("Unable to write metadata store page.");
            }
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
        try (var flushEvent = pageCacheTracer.beginFileFlush()) {
            flush(flushEvent, cursorContext);
        }
    }

    private void writeStoreId(PageCursor cursor, StoreId storeId) throws IOException {
        ByteBuffer buffer = allocateBufferForPosition(Position.STORE_ID);
        StoreIdSerialization.serializeWithFixedSize(storeId, buffer);
        buffer.flip();

        while (buffer.hasRemaining()) {
            writeLongRecord(cursor, buffer.getLong());
        }
    }

    private StoreId readStoreId(PageCursor cursor) throws IOException {
        ByteBuffer buffer = allocateBufferForPosition(Position.STORE_ID);
        cursor.mark();
        do {
            cursor.setOffsetToMark();
            buffer.clear();
            while (buffer.hasRemaining()) {
                buffer.putLong(readLongRecord(cursor));
            }
        } while (cursor.shouldRetry());
        buffer.flip();

        return StoreIdSerialization.deserializeWithFixedSize(buffer);
    }

    private void writeLongRecord(PageCursor cursor, long value) {
        cursor.putByte(Record.IN_USE.byteValue());
        cursor.putLong(value);
    }

    private void writeEmptyRecord(PageCursor cursor) {
        cursor.putByte(Record.NOT_IN_USE.byteValue());
        cursor.putLong(0);
    }

    private long readLongRecord(PageCursor cursor) {
        cursor.getByte();
        return cursor.getLong();
    }

    private void readMetadataFile(CursorContext cursorContext) {
        try (var cursor = openPageCursorForReading(0, cursorContext)) {
            if (cursor.next()) {
                UUID metadataExternalUUID;
                UUID metadataDatabaseUUID;
                long metadataLegacyStoreVersion;
                do {
                    cursor.setOffset(0);
                    metadataExternalUUID = new UUID(readLongRecord(cursor), readLongRecord(cursor));
                    cursor.mark();
                    boolean databaseIdInUse = cursor.getByte() == Record.IN_USE.byteValue();
                    cursor.setOffsetToMark();
                    metadataDatabaseUUID = new UUID(readLongRecord(cursor), readLongRecord(cursor));
                    if (!databaseIdInUse) {
                        metadataDatabaseUUID = null;
                    }
                    metadataLegacyStoreVersion = readLongRecord(cursor);
                } while (cursor.shouldRetry());

                if (metadataLegacyStoreVersion != LEGACY_STORE_VERSION_VALUE) {
                    throw new IllegalStateException("Trying to read metadata store in unrecognised format");
                }

                storeId = readStoreId(cursor);
                legacyStoreVersion = metadataLegacyStoreVersion;
                externalStoreUUID = metadataExternalUUID;
                databaseUUID = metadataDatabaseUUID;
            }
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    /**
     * Obtaining access to read or write fields when the store is not started.
     */
    public static FieldAccess getFieldAccess(
            PageCache pageCache, Path neoStore, String databaseName, CursorContext cursorContext) {

        return new FieldAccess(pageCache, neoStore, databaseName, cursorContext);
    }

    private static ByteBuffer allocateBufferForPosition(Position position) {
        return ByteBuffer.allocate(position.slotCount * Long.BYTES);
    }

    /**
     * Access to read or write fields when the store is not started.
     */
    public static class FieldAccess {
        private final PageCache pageCache;
        private final Path neoStore;
        private final String databaseName;
        private final CursorContext cursorContext;

        private FieldAccess(PageCache pageCache, Path neoStore, String databaseName, CursorContext cursorContext) {
            this.pageCache = pageCache;
            this.neoStore = neoStore;
            this.databaseName = databaseName;
            this.cursorContext = cursorContext;
        }

        public StoreId readStoreId() throws IOException {
            ByteBuffer buffer = allocateBufferForPosition(Position.STORE_ID);
            if (!readValue(Position.STORE_ID, buffer)) {
                // Store ID must always be present in a valid metadata store
                throw new IllegalStateException("Trying to read Store ID field from uninitialised metadata store");
            }

            return StoreIdSerialization.deserializeWithFixedSize(buffer);
        }

        public Optional<UUID> readDatabaseUUID() throws IOException {
            var uuid = readUUID(Position.DATABASE_ID);
            return Optional.ofNullable(uuid);
        }

        private UUID readUUID(Position position) throws IOException {
            ByteBuffer buffer = allocateBufferForPosition(position);
            if (!readValue(position, buffer)) {
                return null;
            }

            return new UUID(buffer.getLong(), buffer.getLong());
        }

        /**
         * There is a field with value set to a constant in 5.0+ metadata stores.
         * If the field is not set to the constant it means that the metadata store is either an unmigrated 4.4 store
         * or simply some garbage.
         * This field is very important in migration code to determine if a database store is unmigrated 4.4 store.
         */
        public boolean isLegacyFieldValid() throws IOException {
            ByteBuffer buffer = allocateBufferForPosition(Position.LEGACY_STORE_VERSION);
            if (!readValue(Position.LEGACY_STORE_VERSION, buffer)) {
                return false;
            }

            return LEGACY_STORE_VERSION_VALUE == buffer.getLong();
        }

        public void writeStoreId(StoreId storeId) throws IOException {
            ByteBuffer buffer = allocateBufferForPosition(Position.STORE_ID);
            StoreIdSerialization.serializeWithFixedSize(storeId, buffer);
            buffer.flip();
            writeValue(Position.STORE_ID, buffer);
        }

        private boolean readValue(Position position, ByteBuffer value) throws IOException {
            boolean inUse = false;
            try (PagedFile pagedFile =
                    pageCache.map(neoStore, pageCache.pageSize(), databaseName, REQUIRED_OPTIONS, DISABLED)) {
                if (pagedFile.getLastPageId() < 0) {
                    return false;
                }

                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                    if (!cursor.next()) {
                        return false;
                    }

                    value.mark();

                    do {
                        value.reset();
                        for (int slot = 0; slot < position.slotCount; slot++) {
                            cursor.setOffset(RECORD_SIZE * (position.firstSlotId + slot));
                            inUse = cursor.getByte() == Record.IN_USE.byteValue();
                            if (!inUse) {
                                break;
                            }
                            value.putLong(cursor.getLong());
                        }
                    } while (cursor.shouldRetry());
                }
            }

            value.flip();
            return inUse;
        }

        private void writeValue(Position position, ByteBuffer value) throws IOException {
            try (PagedFile pagedFile = pageCache.map(neoStore, pageCache.pageSize(), databaseName, REQUIRED_OPTIONS)) {
                try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
                    // This should not happen since the cursor is not open with PF_NO_GROW option,
                    // but better safe than sorry.
                    if (!cursor.next()) {
                        throw new IllegalStateException("Failed to write metadata store");
                    }

                    for (int slot = 0; slot < position.slotCount; slot++) {
                        cursor.setOffset(RECORD_SIZE * (position.firstSlotId + slot));
                        cursor.putByte(Record.IN_USE.byteValue());
                        cursor.putLong(value.getLong());
                    }
                }
            }
        }
    }

    public static long lastOccupiedSlot() {
        var lastPosition = Position.values()[Position.values().length - 1];
        return lastPosition.firstSlotId + lastPosition.slotCount - 1;
    }
}
