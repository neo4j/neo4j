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

import org.eclipse.collections.api.set.ImmutableSet;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.util.Bits;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import static java.lang.String.format;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.CHECKPOINT_LOG_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.DATABASE_ID_LEAST_SIGN_BITS;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.DATABASE_ID_MOST_SIGN_BITS;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.EXTERNAL_STORE_UUID_LEAST_SIGN_BITS;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.EXTERNAL_STORE_UUID_MOST_SIGN_BITS;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.KERNEL_VERSION;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.RECORD_SIZE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.ALWAYS;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class MetaDataStore extends CommonAbstractStore<MetaDataRecord,NoStoreHeader> implements MetadataProvider
{
    private static final String TYPE_DESCRIPTOR = "NeoStore";
    // This value means the field has not been refreshed from the store. Normally, this should happen only once
    static final long FIELD_NOT_INITIALIZED = Long.MIN_VALUE;
    private static final String METADATA_REFRESH_TAG = "metadataRefresh";
    private static final UUID NOT_INITIALIZED_UUID = new UUID( FIELD_NOT_INITIALIZED, FIELD_NOT_INITIALIZED );

    // Positions of meta-data records
    public enum Position
    {
        TIME( 0, "Creation time" ),
        RANDOM_NUMBER( 1, "Random number for store id" ),
        LOG_VERSION( 2, "Current log version" ),
        LAST_TRANSACTION_ID( 3, "Last committed transaction" ),
        STORE_VERSION( 4, "Store format version" ),
        // Obsolete field was used to store first graph property, keep it to avoid conflicts and migrations
        FIRST_GRAPH_PROPERTY( 5, "First property record containing graph properties" ),
        LAST_CONSTRAINT_TRANSACTION( 6, "Last committed transaction containing constraint changes" ),
        @Deprecated() //Will stop being updated in the future since it no longer serves any functionality.
        UPGRADE_TRANSACTION_ID( 7, "Transaction id most recent upgrade was performed at" ),
        @Deprecated() //Will stop being updated in the future since it no longer serves any functionality.
        UPGRADE_TIME( 8, "Time of last upgrade" ),
        LAST_TRANSACTION_CHECKSUM( 9, "Checksum of last committed transaction" ),
        @Deprecated() //Will stop being updated in the future since it no longer serves any functionality.
        UPGRADE_TRANSACTION_CHECKSUM( 10, "Checksum of transaction id the most recent upgrade was performed at" ),
        LAST_CLOSED_TRANSACTION_LOG_VERSION( 11, "Log version where the last transaction commit entry has been written into" ),
        LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET( 12, "Byte offset in the log file where the last transaction commit entry " +
                                                     "has been written into" ),
        LAST_TRANSACTION_COMMIT_TIMESTAMP( 13, "Commit time timestamp for last committed transaction" ),
        @Deprecated() //Will stop being updated in the future since it no longer serves any functionality.
        UPGRADE_TRANSACTION_COMMIT_TIMESTAMP( 14, "Commit timestamp of transaction the most recent upgrade was performed at" ),
        LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP( 15, "Timestamp of last attempt to perform a recovery on the store with missing files." ),
        EXTERNAL_STORE_UUID_MOST_SIGN_BITS( 16, "Database identifier exposed as external store identity. " +
                "Generated on creation and never updated. Most significant bits." ),
        EXTERNAL_STORE_UUID_LEAST_SIGN_BITS( 17, "Database identifier exposed as external store identity. " +
                "Generated on creation and never updated. Least significant bits" ),
        CHECKPOINT_LOG_VERSION( 18, "Current checkpoint log version" ),
        // This field is changed using explicit upgrade trigger. Store can be upgraded to a later version, but still write commands for an older version
        KERNEL_VERSION( 19, "The kernel version (also transaction log version) that is currently being used when writing new transactions" ),
        DATABASE_ID_MOST_SIGN_BITS( 20, "The last used DatabaseId for this database. Most significant bits" ),
        DATABASE_ID_LEAST_SIGN_BITS( 21, "The last used DatabaseId for this database. Least significant bits" );

        private final int id;
        private final String description;

        Position( int id, String description )
        {
            this.id = id;
            this.description = description;
        }

        public int id()
        {
            return id;
        }

        public String description()
        {
            return description;
        }
    }

    // Fields the neostore keeps cached and must be initialized on startup
    private volatile long creationTimeField = FIELD_NOT_INITIALIZED;
    private volatile long randomNumberField = FIELD_NOT_INITIALIZED;
    private volatile long versionField = FIELD_NOT_INITIALIZED;
    private volatile long checkpointLogVersionField = FIELD_NOT_INITIALIZED;
    // This is an atomic long since we, when incrementing last tx id, won't set the record in the page,
    // we do that when flushing, which performs better and fine from a recovery POV.
    private final AtomicLong lastCommittingTxField = new AtomicLong( FIELD_NOT_INITIALIZED );
    private volatile long storeVersionField = FIELD_NOT_INITIALIZED;
    private volatile long latestConstraintIntroducingTxField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTxIdField = FIELD_NOT_INITIALIZED;
    private volatile int upgradeTxChecksumField = (int) FIELD_NOT_INITIALIZED;
    private volatile long upgradeTimeField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeCommitTimestampField = FIELD_NOT_INITIALIZED;
    private volatile UUID externalStoreUUID = NOT_INITIALIZED_UUID;
    private volatile long kernelVersion = FIELD_NOT_INITIALIZED;
    private final PageCacheTracer pageCacheTracer;

    private volatile TransactionId upgradeTransaction = new TransactionId( FIELD_NOT_INITIALIZED, (int) FIELD_NOT_INITIALIZED, FIELD_NOT_INITIALIZED );

    // This is not a field in the store, but something keeping track of which is the currently highest
    // committed transaction id, together with its checksum.
    private final HighestTransactionId highestCommittedTransaction =
            new HighestTransactionId( FIELD_NOT_INITIALIZED, (int) FIELD_NOT_INITIALIZED, FIELD_NOT_INITIALIZED );

    // This is not a field in the store, but something keeping track of which of the committed
    // transactions have been closed. Useful in rotation and shutdown.
    private final OutOfOrderSequence lastClosedTx = new ArrayQueueOutOfOrderSequence( -1, 200, new long[2] );

    // We use these objects and their monitors as "entity" locks on the records, because page write locks are not
    // exclusive. Therefor, these locks are only used when *writing* records, not when reading them.
    private final Object upgradeTimeLock = new Object();
    private final Object creationTimeLock  = new Object();
    private final Object randomNumberLock = new Object();
    private final Object upgradeTransactionLock = new Object();
    private final Object logVersionLock = new Object();
    private final Object checkpointLogVersionLock = new Object();
    private final Object storeVersionLock = new Object();
    private final Object lastConstraintIntroducingTxLock = new Object();
    private final Object transactionCommittedLock = new Object();
    private final Object transactionClosedLock = new Object();

    private volatile boolean closed;

    MetaDataStore( Path file, Path idFile, Config conf,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache, LogProvider logProvider, RecordFormat<MetaDataRecord> recordFormat,
            String storeVersion, PageCacheTracer pageCacheTracer,
            String databaseName,
            ImmutableSet<OpenOption> openOptions )
    {
        super( file, idFile, conf, IdType.NEOSTORE_BLOCK, idGeneratorFactory, pageCache, logProvider,
                TYPE_DESCRIPTOR, recordFormat, NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT, storeVersion, databaseName, openOptions );
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    protected void initialiseNewStoreFile( PageCursorTracer cursorTracer ) throws IOException
    {
        super.initialiseNewStoreFile( cursorTracer );

        long storeVersionAsLong = MetaDataStore.versionStringToLong( storeVersion );
        StoreId storeId = new StoreId( storeVersionAsLong );

        setCreationTime( storeId.getCreationTime(), cursorTracer );
        setRandomNumber( storeId.getRandomId(), cursorTracer );
        // If metaDataStore.creationTime == metaDataStore.upgradeTime && metaDataStore.upgradeTransactionId == BASE_TX_ID
        // then store has never been upgraded
        setUpgradeTime( storeId.getCreationTime(), cursorTracer );
        setUpgradeTransaction( BASE_TX_ID, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP, cursorTracer );
        setCurrentLogVersion( 0, cursorTracer );
        setLastCommittedAndClosedTransactionId( BASE_TX_ID, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP, BASE_TX_LOG_BYTE_OFFSET, BASE_TX_LOG_VERSION,
                cursorTracer );
        setStoreVersion( storeVersionAsLong, cursorTracer );
        setLatestConstraintIntroducingTx( 0, cursorTracer );
        setExternalStoreUUID( UUID.randomUUID(), cursorTracer );
        setCheckpointLogVersion( 0, cursorTracer );
        setKernelVersion( KernelVersion.LATEST, cursorTracer );
        setDatabaseIdUuid( NOT_INITIALIZED_UUID, cursorTracer );

        initHighId();
        flush( cursorTracer );
    }

    public void setKernelVersion( KernelVersion kernelVersion, PageCursorTracer cursorTracer )
    {
        assertNotClosed();
        byte version = kernelVersion.version();
        setRecord( KERNEL_VERSION, version, cursorTracer );
        this.kernelVersion = version;
    }

    @Override
    public KernelVersion kernelVersion()
    {
        assertNotClosed();
        checkInitialized( kernelVersion );
        return KernelVersion.getForVersion( Numbers.safeCastLongToByte( kernelVersion ) );
    }

    @Override
    public long getCheckpointLogVersion()
    {
        assertNotClosed();
        checkInitialized( checkpointLogVersionField );
        return checkpointLogVersionField;
    }

    @Override
    public void setCheckpointLogVersion( long version, PageCursorTracer cursorTracer )
    {
        synchronized ( checkpointLogVersionLock )
        {
            setRecord( CHECKPOINT_LOG_VERSION, version, cursorTracer );
            checkpointLogVersionField = version;
        }
    }

    @Override
    public long incrementAndGetCheckpointLogVersion( PageCursorTracer cursorTracer )
    {
        long checkPointVersion = incrementAndGetVersion( cursorTracer, checkpointLogVersionLock, CHECKPOINT_LOG_VERSION );
        checkpointLogVersionField = checkPointVersion;
        return checkPointVersion;
    }

    private void setExternalStoreUUID( UUID uuid, PageCursorTracer cursorTracer )
    {
        assertNotClosed();
        setRecord( EXTERNAL_STORE_UUID_MOST_SIGN_BITS, uuid.getMostSignificantBits(), cursorTracer );
        setRecord( EXTERNAL_STORE_UUID_LEAST_SIGN_BITS, uuid.getLeastSignificantBits(), cursorTracer );
    }

    // Only for initialization and recovery, so we don't need to lock the records

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId, int checksum, long commitTimestamp, long byteOffset, long logVersion,
            PageCursorTracer cursorTracer )
    {
        assertNotClosed();
        setRecord( Position.LAST_TRANSACTION_ID, transactionId, cursorTracer );
        setRecord( Position.LAST_TRANSACTION_CHECKSUM, checksum, cursorTracer );
        setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, logVersion, cursorTracer );
        setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, byteOffset, cursorTracer );
        setRecord( Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, commitTimestamp, cursorTracer );
        checkInitialized( lastCommittingTxField.get() );
        lastCommittingTxField.set( transactionId );
        lastClosedTx.set( transactionId, new long[]{logVersion, byteOffset} );
        highestCommittedTransaction.set( transactionId, checksum, commitTimestamp );
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
     * @param cursorTracer underlying page cursor tracer.
     * @return the previous value before writing.
     * @throws IOException if any I/O related error occurs.
     */
    public static long setRecord( PageCache pageCache, Path neoStore, Position position, long value, String databaseName, PageCursorTracer cursorTracer )
            throws IOException
    {
        long previousValue = FIELD_NOT_INITIALIZED;
        int pageSize = pageCache.pageSize();
        try ( PagedFile pagedFile = pageCache.map( neoStore, pageSize, databaseName, immutable.empty() ) )
        {
            int offset = offset( position );
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer ) )
            {
                if ( cursor.next() )
                {
                    // We're overwriting a record, get the previous value
                    cursor.setOffset( offset );
                    byte inUse = cursor.getByte();
                    long record = cursor.getLong();

                    if ( inUse == Record.IN_USE.byteValue() )
                    {
                        previousValue = record;
                    }

                    // Write the value
                    cursor.setOffset( offset );
                    cursor.putByte( Record.IN_USE.byteValue() );
                    cursor.putLong( value );
                    if ( cursor.checkAndClearBoundsFlag() )
                    {
                        MetaDataRecord neoStoreRecord = new MetaDataRecord();
                        neoStoreRecord.setId( position.id );
                        throw new UnderlyingStorageException( buildOutOfBoundsExceptionMessage(
                                neoStoreRecord, 0, offset, RECORD_SIZE, pageSize, neoStore.toAbsolutePath().toString() ) );
                    }
                }
            }
        }
        return previousValue;
    }

    private void initHighId()
    {
        Position[] values = Position.values();
        long highestPossibleId = values[values.length - 1].id;
        setHighestPossibleIdInUse( highestPossibleId );
    }

    private static int offset( Position position )
    {
        return RECORD_SIZE * position.id;
    }

    /**
     * Reads a record from a neostore file.
     *
     * @param pageCache {@link PageCache} the {@code neostore} file lives in.
     * @param neoStore {@link Path} pointing to the neostore.
     * @param position record {@link Position}.
     * @param cursorTracer underlying page cursor tracer.
     * @return the read record value specified by {@link Position}.
     */
    public static long getRecord( PageCache pageCache, Path neoStore, Position position, String databaseName, PageCursorTracer cursorTracer ) throws IOException
    {
        var recordFormat = new MetaDataRecordFormat();
        int pageSize = pageCache.pageSize();
        long value = FIELD_NOT_PRESENT;
        try ( PagedFile pagedFile = pageCache.map( neoStore, pageSize, databaseName, immutable.empty() ) )
        {
            if ( pagedFile.getLastPageId() >= 0 )
            {
                try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
                {
                    if ( cursor.next() )
                    {
                        MetaDataRecord record = new MetaDataRecord();
                        record.setId( position.id );
                        do
                        {
                            recordFormat.read( record, cursor, RecordLoad.CHECK, RECORD_SIZE, pageSize / RECORD_SIZE );
                            if ( record.inUse() )
                            {
                                value = record.getValue();
                            }
                            else
                            {
                                value = FIELD_NOT_PRESENT;
                            }
                        }
                        while ( cursor.shouldRetry() );
                        if ( cursor.checkAndClearBoundsFlag() )
                        {
                            int offset = offset( position );
                            throw new UnderlyingStorageException( buildOutOfBoundsExceptionMessage(
                                    record, 0, offset, RECORD_SIZE, pageSize, neoStore.toAbsolutePath().toString() ) );
                        }
                    }
                }
            }
        }
        return value;
    }

    public static void setStoreId( PageCache pageCache, Path neoStore, StoreId storeId, long upgradeTxChecksum, long upgradeTxCommitTimestamp,
            String databaseName, PageCursorTracer cursorTracer ) throws IOException
    {
        setRecord( pageCache, neoStore, Position.TIME, storeId.getCreationTime(), databaseName, cursorTracer );
        setRecord( pageCache, neoStore, Position.RANDOM_NUMBER, storeId.getRandomId(), databaseName, cursorTracer );
        setRecord( pageCache, neoStore, Position.STORE_VERSION, storeId.getStoreVersion(), databaseName, cursorTracer );
        setRecord( pageCache, neoStore, Position.UPGRADE_TIME, storeId.getUpgradeTime(), databaseName, cursorTracer );
        setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_ID, storeId.getUpgradeTxId(), databaseName, cursorTracer );

        setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_CHECKSUM, upgradeTxChecksum, databaseName, cursorTracer );
        setRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP, upgradeTxCommitTimestamp, databaseName, cursorTracer );
    }

    public void setDatabaseIdUuid( UUID uuid, PageCursorTracer cursorTracer )
    {
        assertNotClosed();
        setRecord( DATABASE_ID_MOST_SIGN_BITS, uuid.getMostSignificantBits(), cursorTracer );
        setRecord( DATABASE_ID_LEAST_SIGN_BITS, uuid.getLeastSignificantBits(), cursorTracer );
    }

    public static Optional<UUID> getDatabaseId( PageCache pageCache, Path neoStore, String dabaseName, PageCursorTracer cursorTracer )
    {
        try
        {
            var msb = getRecord( pageCache, neoStore, Position.DATABASE_ID_MOST_SIGN_BITS, dabaseName, cursorTracer );
            var lsb = getRecord( pageCache, neoStore, Position.DATABASE_ID_LEAST_SIGN_BITS, dabaseName, cursorTracer );
            var uuid = new UUID( msb, lsb );
            if ( isNotInitializedUUID( uuid ) || (msb == FIELD_NOT_PRESENT && lsb == FIELD_NOT_PRESENT) )
            {
                return Optional.empty();
            }
            else
            {
                return Optional.of( uuid );
            }
        }
        catch ( IOException ignored )
        {
            return Optional.empty();
        }
    }

    @Override
    public StoreId getStoreId()
    {
        return new StoreId( getCreationTime(), getRandomNumber(), getStoreVersion(), getUpgradeTime(), upgradeTxIdField );
    }

    @Override
    public Optional<ExternalStoreId> getExternalStoreId()
    {
        assertNotClosed();
        var externalStoreUUID = getExternalStoreUUID();
        return isNotInitializedUUID( externalStoreUUID ) ? Optional.empty() : Optional.of( new ExternalStoreId( externalStoreUUID ) );
    }

    public static StoreId getStoreId( PageCache pageCache, Path neoStore, String databaseName, PageCursorTracer cursorTracer ) throws IOException
    {
        return new StoreId(
                getRecord( pageCache, neoStore, Position.TIME, databaseName, cursorTracer ),
                getRecord( pageCache, neoStore, Position.RANDOM_NUMBER, databaseName, cursorTracer ),
                getRecord( pageCache, neoStore, Position.STORE_VERSION, databaseName, cursorTracer ),
                getRecord( pageCache, neoStore, Position.UPGRADE_TIME, databaseName, cursorTracer ),
                getRecord( pageCache, neoStore, Position.UPGRADE_TRANSACTION_ID, databaseName, cursorTracer )
        );
    }

    public long getUpgradeTime()
    {
        assertNotClosed();
        checkInitialized( upgradeTimeField );
        return upgradeTimeField;
    }

    public void setUpgradeTime( long time, PageCursorTracer cursorTracer )
    {
        synchronized ( upgradeTimeLock )
        {
            setRecord( Position.UPGRADE_TIME, time, cursorTracer );
            upgradeTimeField = time;
        }
    }

    public void setUpgradeTransaction( long id, int checksum, long timestamp, PageCursorTracer cursorTracer )
    {
        long pageId = pageIdForRecord( Position.UPGRADE_TRANSACTION_ID.id );
        assert pageId == pageIdForRecord( Position.UPGRADE_TRANSACTION_CHECKSUM.id );
        synchronized ( upgradeTransactionLock )
        {
            try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK, cursorTracer ) )
            {
                if ( !cursor.next() )
                {
                    throw new UnderlyingStorageException( "Could not access MetaDataStore page " + pageId );
                }
                setRecord( cursor, Position.UPGRADE_TRANSACTION_ID, id );
                setRecord( cursor, Position.UPGRADE_TRANSACTION_CHECKSUM, checksum );
                setRecord( cursor, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP, timestamp );
                upgradeTxIdField = id;
                upgradeTxChecksumField = checksum;
                upgradeCommitTimestampField = timestamp;
                upgradeTransaction = new TransactionId( id, checksum, timestamp );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
    }

    private UUID getExternalStoreUUID()
    {
        assertNotClosed();
        if ( isNotInitializedUUID( externalStoreUUID ) )
        {
            refreshFields();
        }
        return externalStoreUUID;
    }

    private static boolean isNotInitializedUUID( UUID uuid )
    {
        return NOT_INITIALIZED_UUID.equals( uuid );
    }

    public long getCreationTime()
    {
        assertNotClosed();
        checkInitialized( creationTimeField );
        return creationTimeField;
    }

    public void setCreationTime( long time, PageCursorTracer cursorTracer )
    {
        synchronized ( creationTimeLock )
        {
            setRecord( Position.TIME, time, cursorTracer );
            creationTimeField = time;
        }
    }

    public long getRandomNumber()
    {
        assertNotClosed();
        checkInitialized( randomNumberField );
        return randomNumberField;
    }

    public void setRandomNumber( long random, PageCursorTracer cursorTracer )
    {
        synchronized ( randomNumberLock )
        {
            setRecord( Position.RANDOM_NUMBER, random, cursorTracer );
            randomNumberField = random;
        }
    }

    @Override
    public long getCurrentLogVersion()
    {
        assertNotClosed();
        checkInitialized( versionField );
        return versionField;
    }

    @Override
    public void setCurrentLogVersion( long version, PageCursorTracer cursorTracer )
    {
        synchronized ( logVersionLock )
        {
            setRecord( Position.LOG_VERSION, version, cursorTracer );
            versionField = version;
        }
    }

    @Override
    public long incrementAndGetVersion( PageCursorTracer cursorTracer )
    {
        long version = incrementAndGetVersion( cursorTracer, logVersionLock, Position.LOG_VERSION );
        versionField = version;
        return version;
    }

    private long incrementAndGetVersion( PageCursorTracer cursorTracer, Object lock, Position position )
    {
        // This method can expect synchronisation at a higher level,
        // and be effectively single-threaded.
        long pageId = pageIdForRecord( position.id );
        long version;
        synchronized ( lock )
        {
            try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK, cursorTracer ) )
            {
                if ( cursor.next() )
                {
                    version = incrementVersion( cursor, position );
                }
                else
                {
                    throw new IllegalStateException( "Filed " + position + "missing in metadata store. Page " + pageId + "not found." );
                }
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
        flush( cursorTracer ); // make sure the new version value is persisted
        return version;
    }

    private long incrementVersion( PageCursor cursor, Position position )
    {
        if ( !cursor.isWriteLocked() )
        {
            throw new IllegalArgumentException( "Cannot increment log version on page cursor that is not write-locked" );
        }
        // offsets plus one to skip the inUse byte
        int offset = (position.id * getRecordSize()) + 1;
        long value = cursor.getLong( offset ) + 1;
        cursor.putLong( offset, value );
        checkForDecodingErrors( cursor, position.id, NORMAL );
        return value;
    }

    public long getStoreVersion()
    {
        assertNotClosed();
        checkInitialized( storeVersionField );
        return storeVersionField;
    }

    public void setStoreVersion( long version, PageCursorTracer cursorTracer )
    {
        synchronized ( storeVersionLock )
        {
            setRecord( Position.STORE_VERSION, version, cursorTracer );
            storeVersionField = version;
        }
    }

    public long getLatestConstraintIntroducingTx()
    {
        assertNotClosed();
        checkInitialized( latestConstraintIntroducingTxField );
        return latestConstraintIntroducingTxField;
    }

    public void setLatestConstraintIntroducingTx( long latestConstraintIntroducingTx, PageCursorTracer cursorTracer )
    {
        synchronized ( lastConstraintIntroducingTxLock )
        {
            setRecord( Position.LAST_CONSTRAINT_TRANSACTION, latestConstraintIntroducingTx, cursorTracer );
            latestConstraintIntroducingTxField = latestConstraintIntroducingTx;
        }
    }

    private void readAllFields( PageCursor cursor ) throws IOException
    {
        do
        {
            creationTimeField = getRecordValue( cursor, Position.TIME );
            randomNumberField = getRecordValue( cursor, Position.RANDOM_NUMBER );
            versionField = getRecordValue( cursor, Position.LOG_VERSION );
            long lastCommittedTxId = getRecordValue( cursor, Position.LAST_TRANSACTION_ID );
            lastCommittingTxField.set( lastCommittedTxId );
            storeVersionField = getRecordValue( cursor, Position.STORE_VERSION );
            getRecordValue( cursor, Position.FIRST_GRAPH_PROPERTY );
            latestConstraintIntroducingTxField = getRecordValue( cursor, Position.LAST_CONSTRAINT_TRANSACTION );
            upgradeTxIdField = getRecordValue( cursor, Position.UPGRADE_TRANSACTION_ID );
            upgradeTxChecksumField = (int) getRecordValue( cursor, Position.UPGRADE_TRANSACTION_CHECKSUM );
            upgradeTimeField = getRecordValue( cursor, Position.UPGRADE_TIME );
            long lastClosedTransactionLogVersion = getRecordValue( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
            long lastClosedTransactionLogByteOffset = getRecordValue( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
            lastClosedTx.set( lastCommittedTxId,
                    new long[]{lastClosedTransactionLogVersion, lastClosedTransactionLogByteOffset} );
            highestCommittedTransaction.set( lastCommittedTxId,
                    (int) getRecordValue( cursor, Position.LAST_TRANSACTION_CHECKSUM ),
                    getRecordValue( cursor, Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, UNKNOWN_TX_COMMIT_TIMESTAMP
                    ) );
            upgradeCommitTimestampField = getRecordValue( cursor, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP,
                    BASE_TX_COMMIT_TIMESTAMP );

            externalStoreUUID = readExternalStoreUUID( cursor );

            upgradeTransaction = new TransactionId( upgradeTxIdField, upgradeTxChecksumField, upgradeCommitTimestampField );
            checkpointLogVersionField = getRecordValue( cursor, CHECKPOINT_LOG_VERSION, 0 );
            kernelVersion = getRecordValue( cursor, KERNEL_VERSION );
        }
        while ( cursor.shouldRetry() );
        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw new UnderlyingStorageException(
                    "Out of page bounds when reading all meta-data fields. The page in question is page " +
                    cursor.getCurrentPageId() + " of file " + storageFile.toAbsolutePath() + ", which is " +
                    cursor.getCurrentPageSize() + " bytes in size" );
        }
    }

    private UUID readExternalStoreUUID( PageCursor cursor )
    {
        long mostSignificantBits = getRecordValue( cursor, EXTERNAL_STORE_UUID_MOST_SIGN_BITS, FIELD_NOT_INITIALIZED );
        long leastSignificantBits = getRecordValue( cursor, EXTERNAL_STORE_UUID_LEAST_SIGN_BITS, FIELD_NOT_INITIALIZED );
        return new UUID( mostSignificantBits, leastSignificantBits );
    }

    long getRecordValue( PageCursor cursor, Position position )
    {
        return getRecordValue( cursor, position, FIELD_NOT_PRESENT );
    }

    private long getRecordValue( PageCursor cursor, Position position, long defaultValue )
    {
        MetaDataRecord record = newRecord();
        try
        {
            record.setId( position.id );
            recordFormat.read( record, cursor, ALWAYS, RECORD_SIZE, getRecordsPerPage() );
            if ( record.inUse() )
            {
                return record.getValue();
            }
            return defaultValue;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void refreshFields()
    {
        scanAllFields( PF_SHARED_READ_LOCK, element ->
        {
            readAllFields( element );
            return false;
        } );
    }

    private void scanAllFields( int pf_flags, Visitor<PageCursor,IOException> visitor )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( METADATA_REFRESH_TAG );
              PageCursor cursor = pagedFile.io( 0, pf_flags, cursorTracer ) )
        {
            if ( cursor.next() )
            {
                visitor.visit( cursor );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void setRecord( Position position, long value, PageCursorTracer cursorTracer )
    {
        MetaDataRecord record = new MetaDataRecord();
        record.initialize( true, value );
        record.setId( position.id );
        updateRecord( record, cursorTracer );
    }

    private void setRecord( PageCursor cursor, Position position, long value )
    {
        if ( !cursor.isWriteLocked() )
        {
            throw new IllegalArgumentException( "Cannot write record without a page cursor that is write-locked" );
        }
        int offset = offsetForId( position.id );
        cursor.setOffset( offset );
        cursor.putByte( Record.IN_USE.byteValue() );
        cursor.putLong( value );
        checkForDecodingErrors( cursor, position.id, NORMAL );
    }

    /*
     * The following two methods encode and decode a string that is presumably
     * the store version into a long via Latin1 encoding. This leaves room for
     * 7 characters and 1 byte for the length. Current string is
     * 0.A.0 which is 5 chars, so we have room for expansion.
     */
    public static long versionStringToLong( String storeVersion )
    {
        if ( CommonAbstractStore.UNKNOWN_VERSION.equals( storeVersion ) )
        {
            return -1;
        }
        Bits bits = Bits.bits( 8 );
        int length = storeVersion.length();
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException( format(
                    "The given string %s is not of proper size for a store version string", storeVersion ) );
        }
        bits.put( length, 8 );
        for ( int i = 0; i < length; i++ )
        {
            char c = storeVersion.charAt( i );
            if ( c >= 256 )
            {
                throw new IllegalArgumentException( format(
                        "Store version strings should be encode-able as Latin1 - %s is not", storeVersion ) );
            }
            bits.put( c, 8 ); // Just the lower byte
        }
        return bits.getLong();
    }

    public static String versionLongToString( long storeVersion )
    {
        if ( storeVersion == -1 )
        {
            return CommonAbstractStore.UNKNOWN_VERSION;
        }
        Bits bits = Bits.bitsFromLongs( new long[]{storeVersion} );
        int length = bits.getShort( 8 );
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException( format( "The read version string length %d is not proper.",
                    length ) );
        }
        char[] result = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (char) bits.getShort( 8 );
        }
        return new String( result );
    }

    @Override
    public long nextCommittingTransactionId()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittingTxField.incrementAndGet();
    }

    @Override
    public long committingTransactionId()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittingTxField.get();
    }

    @Override
    public void transactionCommitted( long transactionId, int checksum, long commitTimestamp, PageCursorTracer cursorTracer )
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        if ( highestCommittedTransaction.offer( transactionId, checksum, commitTimestamp ) )
        {
            // We need to synchronize here in order to guarantee that the three fields are written consistently
            // together. Note that having a write lock on the page is not enough for 3 reasons:
            // 1. page write locks are not exclusive
            // 2. the records might be in different pages
            // 3. some other thread might kick in while we have been written only one record
            synchronized ( transactionCommittedLock )
            {
                // Double-check with highest tx id under the lock, so that there haven't been
                // another higher transaction committed between our id being accepted and
                // acquiring this monitor.
                if ( highestCommittedTransaction.get().transactionId() == transactionId )
                {
                    long pageId = pageIdForRecord( Position.LAST_TRANSACTION_ID.id );
                    assert pageId == pageIdForRecord( Position.LAST_TRANSACTION_CHECKSUM.id );
                    try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK, cursorTracer ) )
                    {
                        if ( cursor.next() )
                        {
                            setRecord( cursor, Position.LAST_TRANSACTION_ID, transactionId );
                            setRecord( cursor, Position.LAST_TRANSACTION_CHECKSUM, checksum );
                            setRecord( cursor, Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, commitTimestamp );
                        }
                    }
                    catch ( IOException e )
                    {
                        throw new UnderlyingStorageException( e );
                    }
                }
            }
        }
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return highestCommittedTransaction.get().transactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return highestCommittedTransaction.get();
    }

    @Override
    public TransactionId getUpgradeTransaction()
    {
        assertNotClosed();
        checkInitialized( upgradeTxIdField );
        return upgradeTransaction;
    }

    @Override
    public long getLastClosedTransactionId()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastClosedTx.getHighestGapFreeNumber();
    }

    @Override
    public long[] getLastClosedTransaction()
    {
        assertNotClosed();
        checkInitialized( lastCommittingTxField.get() );
        return lastClosedTx.get();
    }

    /**
     * Ensures that all fields are read from the store, by checking the initial value of the field in question
     *
     * @param field the value
     */
    private void checkInitialized( long field )
    {
        if ( field == FIELD_NOT_INITIALIZED )
        {
            refreshFields();
        }
    }

    @Override
    public void transactionClosed( long transactionId, long logVersion, long byteOffset, PageCursorTracer cursorTracer )
    {
        if ( lastClosedTx.offer( transactionId, new long[]{logVersion, byteOffset} ) )
        {
            long pageId = pageIdForRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION.id );
            assert pageId == pageIdForRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET.id );
            synchronized ( transactionClosedLock )
            {
                try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK, cursorTracer ) )
                {
                    if ( cursor.next() )
                    {
                        long[] lastClosedTransactionData = lastClosedTx.get();
                        setRecord( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, lastClosedTransactionData[1] );
                        setRecord( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, lastClosedTransactionData[2] );
                    }
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }
        }
    }

    @Override
    public void resetLastClosedTransaction( long transactionId, long logVersion, long byteOffset, boolean missingLogs, PageCursorTracer cursorTracer )
    {
        assertNotClosed();
        setRecord( Position.LAST_TRANSACTION_ID, transactionId, cursorTracer );
        setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, logVersion, cursorTracer );
        setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, byteOffset, cursorTracer );
        if ( missingLogs )
        {
            setRecord( Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP, System.currentTimeMillis(), cursorTracer );
        }
        lastClosedTx.set( transactionId, new long[]{logVersion, byteOffset} );
    }

    public void logRecords( final DiagnosticsLogger logger )
    {
        scanAllFields( PF_SHARED_READ_LOCK, cursor ->
        {
            for ( Position position : Position.values() )
            {
                long value;
                do
                {
                    value = getRecordValue( cursor, position );
                }
                while ( cursor.shouldRetry() );
                boolean bounds = cursor.checkAndClearBoundsFlag();
                logger.log( position.name() + " (" + position.description() + "): " + value +
                            (bounds ? " (out-of-bounds detected; value cannot be trusted)" : ""));
            }
            return false;
        } );
    }

    @Override
    public MetaDataRecord newRecord()
    {
        return new MetaDataRecord();
    }

    @Override
    public void prepareForCommit( MetaDataRecord record, PageCursorTracer cursorTracer )
    {   // No need to do anything with these records before commit
    }

    @Override
    public void prepareForCommit( MetaDataRecord record, IdSequence idSequence, PageCursorTracer cursorTracer )
    {   // No need to do anything with these records before commit
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        finally
        {
            closed = true;
        }
    }

    private void assertNotClosed()
    {
        if ( closed )
        {
            throw new StoreFileClosedException( this, storageFile );
        }
    }
}
