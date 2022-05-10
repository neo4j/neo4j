/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import static java.lang.String.valueOf;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.internal.id.EmptyIdGeneratorFactory.EMPTY_ID_GENERATOR_FACTORY;
import static org.neo4j.io.pagecache.IOController.DISABLED;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.RECORD_SIZE;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.StoreFileClosedException;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.LegacyStoreId;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.util.HighestTransactionId;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

public class MetaDataStore extends CommonAbstractStore<MetaDataRecord, NoStoreHeader> implements MetadataProvider {
    private static final String TYPE_DESCRIPTOR = "NeoStore";
    private static final long NOT_INITIALIZED = Long.MIN_VALUE;
    static final UUID NOT_INITIALIZED_UUID = new UUID(NOT_INITIALIZED, NOT_INITIALIZED);

    // MetaDataStore always big-endian so we can read store version regardless of endianness of other stores
    private static final ImmutableSet<OpenOption> REQUIRED_OPTIONS = immutable.of(PageCacheOpenOptions.BIG_ENDIAN);

    // Positions of meta-data records
    public enum Position {
        EXTERNAL_STORE_UUID_MOST_SIGN_BITS(
                0,
                "Database identifier exposed as external store identity. "
                        + "Generated on creation and never updated. Most significant bits."),
        EXTERNAL_STORE_UUID_LEAST_SIGN_BITS(
                1,
                "Database identifier exposed as external store identity. "
                        + "Generated on creation and never updated. Least significant bits"),
        DATABASE_ID_MOST_SIGN_BITS(2, "The last used DatabaseId for this database. Most significant bits"),
        DATABASE_ID_LEAST_SIGN_BITS(3, "The last used DatabaseId for this database. Least significant bits"),
        STORE_VERSION(4, "Store format version"),
        TIME(5, "Creation time"),
        RANDOM_NUMBER(6, "Random number for store id");
        public static final Position[] POSITIONS_VALUES = Position.values();

        private final int id;
        private final String description;

        Position(int id, String description) {
            this.id = id;
            this.description = description;
        }

        public int id() {
            return id;
        }

        public String description() {
            return description;
        }
    }

    private volatile long creationTime = NOT_INITIALIZED;
    private volatile long randomNumber = NOT_INITIALIZED;
    private volatile long storeIdStoreVersion = NOT_INITIALIZED;
    private volatile UUID externalStoreUUID;
    private volatile UUID databaseUUID;

    private final AtomicLong logVersion = new AtomicLong();
    private final AtomicLong checkpointLogVersion = new AtomicLong();
    private final AtomicLong lastCommittingTx = new AtomicLong(NOT_INITIALIZED);
    private volatile long latestConstraintIntroducingTxId;
    private volatile KernelVersion kernelVersion;

    // This is not a field in the store, but something keeping track of which is the currently highest
    // committed transaction id, together with its checksum.
    private final HighestTransactionId highestCommittedTransaction =
            new HighestTransactionId(NOT_INITIALIZED, (int) NOT_INITIALIZED, NOT_INITIALIZED);

    // This is not a field in the store, but something keeping track of which of the committed
    // transactions have been closed. Useful in rotation and shutdown.
    private final OutOfOrderSequence lastClosedTx = new ArrayQueueOutOfOrderSequence(-1, 200, new long[4]);
    private volatile boolean closed;

    MetaDataStore(
            Path file,
            Config conf,
            PageCache pageCache,
            InternalLogProvider logProvider,
            RecordFormat<MetaDataRecord> recordFormat,
            String storeVersion,
            DatabaseReadOnlyChecker readOnlyChecker,
            LogTailMetadata logTailMetadata,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        super(
                file,
                null,
                conf,
                null,
                EMPTY_ID_GENERATOR_FACTORY,
                pageCache,
                logProvider,
                TYPE_DESCRIPTOR,
                recordFormat,
                NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT,
                storeVersion,
                readOnlyChecker,
                databaseName,
                openOptions);

        checkpointLogVersion.set(logTailMetadata.getCheckpointLogVersion());
        kernelVersion = logTailMetadata.getKernelVersion();
        logVersion.set(logTailMetadata.getLogVersion());
        var lastCommittedTx = logTailMetadata.getLastCommittedTransaction();
        lastCommittingTx.set(lastCommittedTx.transactionId());
        highestCommittedTransaction.set(
                lastCommittedTx.transactionId(), lastCommittedTx.checksum(), lastCommittedTx.commitTimestamp());
        var logPosition = logTailMetadata.getLastTransactionLogPosition();
        lastClosedTx.set(lastCommittedTx.transactionId(), new long[] {
            logPosition.getLogVersion(),
            logPosition.getByteOffset(),
            lastCommittedTx.checksum(),
            lastCommittedTx.commitTimestamp()
        });
    }

    @Override
    protected void initialiseNewStoreFile(CursorContext cursorContext) throws IOException {
        super.initialiseNewStoreFile(cursorContext);
        LegacyStoreId storeId = new LegacyStoreId(StoreVersion.versionStringToLong(storeVersion));
        generateMetadataFile(storeId, UUID.randomUUID(), NOT_INITIALIZED_UUID, cursorContext);
    }

    @Override
    protected void initialise(boolean createIfNotExists, CursorContextFactory contextFactory) {
        super.initialise(createIfNotExists, contextFactory);
        try (CursorContext context = contextFactory.create("readMetadata")) {
            readMetadataFile(context);
        }
    }

    @Override
    public long getHighId() {
        Position[] values = Position.POSITIONS_VALUES;
        return values[values.length - 1].id + 1;
    }

    public void setKernelVersion(KernelVersion kernelVersion) {
        assertNotClosed();
        this.kernelVersion = kernelVersion;
    }

    @Override
    public KernelVersion kernelVersion() {
        assertNotClosed();
        return kernelVersion;
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
            long transactionId, int checksum, long commitTimestamp, long byteOffset, long logVersion) {
        assertNotClosed();
        lastCommittingTx.set(transactionId);
        lastClosedTx.set(transactionId, new long[] {logVersion, byteOffset, checksum, commitTimestamp});
        highestCommittedTransaction.set(transactionId, checksum, commitTimestamp);
    }

    /**
     * Writes a record in a neostore file.
     * This method only works for neostore files of the current version.
     *
     * @param pageCache {@link PageCache} the {@code neostore} file lives in.
     * @param neoStore {@link Path} pointing to the neostore.
     * @param position record {@link Position}.
     * @param value value to write in that record.
     * @param databaseName name of database this metadata store belongs to.
     * @param cursorContext underlying page cursor context.
     * @return the previous value before writing.
     * @throws IOException if any I/O related error occurs.
     */
    public static long setRecord(
            PageCache pageCache,
            Path neoStore,
            Position position,
            long value,
            String databaseName,
            CursorContext cursorContext)
            throws IOException {
        long previousValue = NOT_INITIALIZED;
        int pageSize = pageCache.pageSize();
        try (PagedFile pagedFile = pageCache.map(neoStore, pageSize, databaseName, REQUIRED_OPTIONS)) {
            int offset = offset(position);
            try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
                if (cursor.next()) {
                    // We're overwriting a record, get the previous value
                    cursor.setOffset(offset);
                    byte inUse = cursor.getByte();
                    long record = cursor.getLong();

                    if (inUse == Record.IN_USE.byteValue()) {
                        previousValue = record;
                    }

                    // Write the value
                    cursor.setOffset(offset);
                    cursor.putByte(Record.IN_USE.byteValue());
                    cursor.putLong(value);
                    if (cursor.checkAndClearBoundsFlag()) {
                        MetaDataRecord neoStoreRecord = new MetaDataRecord();
                        neoStoreRecord.setId(position.id);
                        throw new UnderlyingStorageException(buildOutOfBoundsExceptionMessage(
                                neoStoreRecord,
                                0,
                                offset,
                                RECORD_SIZE,
                                pageSize,
                                neoStore.toAbsolutePath().toString()));
                    }
                }
            }
        }
        return previousValue;
    }

    private static int offset(Position position) {
        return RECORD_SIZE * position.id;
    }

    /**
     * Reads a record from a neostore file.
     *
     * @param pageCache {@link PageCache} the {@code neostore} file lives in.
     * @param neoStore {@link Path} pointing to the neostore.
     * @param position record {@link Position}.
     * @param cursorContext underlying page cursor context.
     * @return the read record value specified by {@link Position}.
     */
    public static long getRecord(
            PageCache pageCache, Path neoStore, Position position, String databaseName, CursorContext cursorContext)
            throws IOException {
        var recordFormat = new MetaDataRecordFormat();
        int payloadSize = pageCache.payloadSize();
        long value = FIELD_NOT_PRESENT;
        try (PagedFile pagedFile =
                pageCache.map(neoStore, pageCache.pageSize(), databaseName, REQUIRED_OPTIONS, DISABLED)) {
            if (pagedFile.getLastPageId() >= 0) {
                try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                    if (cursor.next()) {
                        MetaDataRecord record = new MetaDataRecord();
                        record.setId(position.id);
                        do {
                            recordFormat.read(record, cursor, RecordLoad.CHECK, RECORD_SIZE, payloadSize / RECORD_SIZE);
                            if (record.inUse()) {
                                value = record.getValue();
                            } else {
                                value = FIELD_NOT_PRESENT;
                            }
                        } while (cursor.shouldRetry());
                        if (cursor.checkAndClearBoundsFlag()) {
                            int offset = offset(position);
                            throw new UnderlyingStorageException(buildOutOfBoundsExceptionMessage(
                                    record,
                                    0,
                                    offset,
                                    RECORD_SIZE,
                                    payloadSize,
                                    neoStore.toAbsolutePath().toString()));
                        }
                    }
                }
            }
        }
        return value;
    }

    @Override
    public void regenerateMetadata(LegacyStoreId storeId, UUID externalStoreUUID, CursorContext cursorContext) {
        generateMetadataFile(storeId, externalStoreUUID, NOT_INITIALIZED_UUID, cursorContext);
        readMetadataFile(cursorContext);
    }

    @Override
    public void setDatabaseIdUuid(UUID uuid, CursorContext cursorContext) {
        assertNotClosed();
        generateMetadataFile(getLegacyStoreId(), externalStoreUUID, uuid, cursorContext);
        readMetadataFile(cursorContext);
    }

    public static Optional<UUID> getDatabaseIdUuid(
            PageCache pageCache, Path neoStore, String databaseName, CursorContext cursorContext) {
        try {
            long msb = getRecord(pageCache, neoStore, Position.DATABASE_ID_MOST_SIGN_BITS, databaseName, cursorContext);
            long lsb =
                    getRecord(pageCache, neoStore, Position.DATABASE_ID_LEAST_SIGN_BITS, databaseName, cursorContext);
            var uuid = new UUID(msb, lsb);
            return wrapDatabaseIdUuid(uuid);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<UUID> getDatabaseIdUuid(CursorContext cursorContext) {
        assertNotClosed();
        var databaseUUID = this.databaseUUID;
        return isNotInitializedUUID(databaseUUID) ? Optional.empty() : Optional.of(databaseUUID);
    }

    @Override
    public LegacyStoreId getLegacyStoreId() {
        return new LegacyStoreId(getCreationTime(), getRandomNumber(), getStoreVersion());
    }

    @Override
    public StoreId getStoreId() {
        return LegacyMetadataHandler.storeIdFromLegacyMetadata(getCreationTime(), getRandomNumber(), getStoreVersion());
    }

    @Override
    public Optional<ExternalStoreId> getExternalStoreId() {
        assertNotClosed();
        var externalStoreUUID = this.externalStoreUUID;
        return isNotInitializedUUID(externalStoreUUID)
                ? Optional.empty()
                : Optional.of(new ExternalStoreId(externalStoreUUID));
    }

    public static LegacyStoreId getStoreId(
            PageCache pageCache, Path neoStore, String databaseName, CursorContext cursorContext) throws IOException {
        return new LegacyStoreId(
                getRecord(pageCache, neoStore, Position.TIME, databaseName, cursorContext),
                getRecord(pageCache, neoStore, Position.RANDOM_NUMBER, databaseName, cursorContext),
                getRecord(pageCache, neoStore, Position.STORE_VERSION, databaseName, cursorContext));
    }

    public long getCreationTime() {
        assertNotClosed();
        return creationTime;
    }

    public long getRandomNumber() {
        assertNotClosed();
        return randomNumber;
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

    public long getStoreVersion() {
        assertNotClosed();
        return storeIdStoreVersion;
    }

    public long getLatestConstraintIntroducingTx() {
        assertNotClosed();
        return latestConstraintIntroducingTxId;
    }

    public void setLatestConstraintIntroducingTx(long latestConstraintIntroducingTxId) {
        this.latestConstraintIntroducingTxId = latestConstraintIntroducingTxId;
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
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp) {
        assertNotClosed();
        highestCommittedTransaction.offer(transactionId, checksum, commitTimestamp);
    }

    @Override
    public long getLastCommittedTransactionId() {
        assertNotClosed();
        return highestCommittedTransaction.get().transactionId();
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
    public ClosedTransactionMetadata getLastClosedTransaction() {
        assertNotClosed();
        long[] txData = lastClosedTx.get();
        return new ClosedTransactionMetadata(
                txData[0], new LogPosition(txData[1], txData[2]), (int) txData[3], txData[4]);
    }

    @Override
    public void transactionClosed(
            long transactionId, long logVersion, long byteOffset, int checksum, long commitTimestamp) {
        lastClosedTx.offer(transactionId, new long[] {logVersion, byteOffset, checksum, commitTimestamp});
    }

    @Override
    public void resetLastClosedTransaction(
            long transactionId, long logVersion, long byteOffset, int checksum, long commitTimestamp) {
        assertNotClosed();
        lastClosedTx.set(transactionId, new long[] {logVersion, byteOffset, checksum, commitTimestamp});
    }

    public void logRecords(final DiagnosticsLogger logger) {
        for (Position position : Position.POSITIONS_VALUES) {
            var logRecord =
                    switch (position) {
                        case TIME -> new PositionLogRecord(creationTime);
                        case RANDOM_NUMBER -> new PositionLogRecord(randomNumber);
                        case STORE_VERSION -> new PositionLogRecord(
                                valueOf(storeIdStoreVersion),
                                " (" + StoreVersion.versionLongToString(storeIdStoreVersion) + ")");
                        case EXTERNAL_STORE_UUID_MOST_SIGN_BITS -> new PositionLogRecord(
                                externalStoreUUID.getMostSignificantBits());
                        case EXTERNAL_STORE_UUID_LEAST_SIGN_BITS -> new PositionLogRecord(
                                externalStoreUUID.getLeastSignificantBits());
                        case DATABASE_ID_MOST_SIGN_BITS -> new PositionLogRecord(databaseUUID.getMostSignificantBits());
                        case DATABASE_ID_LEAST_SIGN_BITS -> new PositionLogRecord(
                                databaseUUID.getLeastSignificantBits());
                    };

            logger.log(position.name() + " (" + position.description() + "): " + logRecord);
        }
    }

    @Override
    public MetaDataRecord newRecord() {
        return new MetaDataRecord();
    }

    @Override
    public void prepareForCommit(
            MetaDataRecord record,
            CursorContext cursorContext) { // No need to do anything with these records before commit
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
            LegacyStoreId storeId, UUID externalStoreUUID, UUID databaseUUID, CursorContext cursorContext) {
        try (var cursor = openPageCursorForWriting(0, cursorContext)) {
            if (cursor.next()) {
                writeLongRecord(cursor, externalStoreUUID.getMostSignificantBits());
                writeLongRecord(cursor, externalStoreUUID.getLeastSignificantBits());
                writeLongRecord(cursor, databaseUUID.getMostSignificantBits());
                writeLongRecord(cursor, databaseUUID.getLeastSignificantBits());
                writeLongRecord(cursor, storeId.getStoreVersion());
                writeLongRecord(cursor, storeId.getCreationTime());
                writeLongRecord(cursor, storeId.getRandomId());
            } else {
                throw new IllegalStateException("Unable to write metadata store page.");
            }
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
        flush(cursorContext);
    }

    private void writeLongRecord(PageCursor cursor, long value) {
        cursor.putByte(Record.IN_USE.byteValue());
        cursor.putLong(value);
    }

    private long readLongRecord(PageCursor cursor) {
        cursor.getByte();
        return cursor.getLong();
    }

    private void readMetadataFile(CursorContext cursorContext) {
        try (var cursor = openPageCursorForReading(0, cursorContext)) {
            if (cursor.next()) {
                LegacyStoreId metadataStoreId;
                UUID metadataExternalUUID;
                UUID metadataDatabaseUUID;
                do {
                    cursor.setOffset(0);
                    metadataExternalUUID = new UUID(readLongRecord(cursor), readLongRecord(cursor));
                    metadataDatabaseUUID = new UUID(readLongRecord(cursor), readLongRecord(cursor));
                    long storeVersion = readLongRecord(cursor);
                    metadataStoreId = new LegacyStoreId(readLongRecord(cursor), readLongRecord(cursor), storeVersion);
                } while (cursor.shouldRetry());

                creationTime = metadataStoreId.getCreationTime();
                randomNumber = metadataStoreId.getRandomId();
                storeIdStoreVersion = metadataStoreId.getStoreVersion();
                externalStoreUUID = metadataExternalUUID;
                databaseUUID = metadataDatabaseUUID;
            }
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    private static Optional<UUID> wrapDatabaseIdUuid(UUID uuid) {
        return isNotInitializedUUID(uuid) ? Optional.empty() : Optional.of(uuid);
    }

    private static boolean isNotInitializedUUID(UUID uuid) {
        return NOT_INITIALIZED_UUID.equals(uuid);
    }

    private record PositionLogRecord(String value, String additionalDescriptor) {
        private PositionLogRecord(long value) {
            this(valueOf(value), EMPTY);
        }

        @Override
        public String toString() {
            return value + additionalDescriptor;
        }
    }
}
