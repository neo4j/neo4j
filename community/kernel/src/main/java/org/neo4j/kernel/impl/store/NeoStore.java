/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.OutOfOrderSequence;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.util.CappedOperation.time;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStore doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" validation
 * performed in there.
 */
public class NeoStore extends AbstractStore implements TransactionIdStore, LogVersionRepository
{
    public abstract static class Configuration
            extends AbstractStore.Configuration
    {
        public static final Setting<Integer> relationship_grab_size = GraphDatabaseSettings.relationship_grab_size;
    }

    public static final String TYPE_DESCRIPTOR = "NeoStore";
    // This value means the field has not been refreshed from the store. Normally, this should happen only once
    public static final long FIELD_NOT_PRESENT = -1;
    public static final long FIELD_NOT_INITIALIZED = Long.MIN_VALUE;
    /*
     *  9 longs in header (long + in use), time | random | version | txid | store version | graph next prop | latest
     *  constraint tx | upgrade time | upgrade id
     */
    public static final int RECORD_SIZE = 9;
    public static final String DEFAULT_NAME = "neostore";
    // Positions of meta-data records

    public enum Position
    {
        TIME( 0, "Creation time" ),
        RANDOM_NUMBER( 1, "Random number for store id" ),
        LOG_VERSION( 2, "Current log version" ),
        LAST_TRANSACTION_ID( 3, "Last committed transaction" ),
        STORE_VERSION( 4, "Store format version" ),
        FIRST_GRAPH_PROPERTY( 5, "First property record containing graph properties" ),
        LAST_CONSTRAINT_TRANSACTION( 6, "Last committed transaction containing constraint changes" ),
        UPGRADE_TRANSACTION_ID( 7, "Transaction id most recent upgrade was performed at" ),
        UPGRADE_TIME( 8, "Time of last upgrade" ),
        LAST_TRANSACTION_CHECKSUM( 9, "Checksum of last committed transaction" ),
        UPGRADE_TRANSACTION_CHECKSUM( 10, "Checksum of transaction id the most recent upgrade was performed at" ),
        LAST_CLOSED_TRANSACTION_LOG_VERSION( 11, "Log version where the last transaction commit entry has been written into" ),
        LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET( 12, "Byte offset in the log file where the last transaction commit entry " +
                                              "has been written into" );

        private final int id;
        private final String description;

        Position( int id, String description )
        {
            this.id = id;
            this.description = description;
        }

        public String description()
        {
            return description;
        }
    }

    public static final int META_DATA_RECORD_COUNT = Position.values().length;

    public static boolean isStorePresent( PageCache pageCache, File storeDir )
    {
        File neoStore = new File( storeDir, DEFAULT_NAME );
        try ( PagedFile file = pageCache.map( neoStore, getPageSize( pageCache ) ) )
        {
            if ( file.getLastPageId() == -1 )
            {
                return false;
            }
        }
        catch ( IOException e )
        {
            return false;
        }

        return true;
    }

    private NodeStore nodeStore;
    private PropertyStore propStore;
    private RelationshipStore relStore;
    private RelationshipTypeTokenStore relTypeStore;
    private LabelTokenStore labelTokenStore;
    private SchemaStore schemaStore;
    private RelationshipGroupStore relGroupStore;
    private CountsTracker counts;

    // Fields the neostore keeps cached and must be initialized on startup
    private volatile long creationTimeField = FIELD_NOT_INITIALIZED;
    private volatile long randomNumberField = FIELD_NOT_INITIALIZED;
    private volatile long versionField = FIELD_NOT_INITIALIZED;
    // This is an atomic long since we, when incrementing last tx id, won't set the record in the page,
    // we do that when flushing, which is more performant and fine from a recovery POV.
    private final AtomicLong lastCommittingTxField = new AtomicLong( FIELD_NOT_INITIALIZED );
    private volatile long storeVersionField = FIELD_NOT_INITIALIZED;
    private volatile long graphNextPropField = FIELD_NOT_INITIALIZED;
    private volatile long latestConstraintIntroducingTxField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTxIdField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTimeField = FIELD_NOT_INITIALIZED;
    private volatile long lastTransactionChecksum = FIELD_NOT_INITIALIZED;
    private volatile long lastClosedTransactionLogVersion = FIELD_NOT_INITIALIZED;
    private volatile long lastClosedTransactionLogByteOffset = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTxChecksumField = FIELD_NOT_INITIALIZED;

    // This is not a field in the store, but something keeping track of which of the committed
    // transactions have been closed. Useful in rotation and shutdown.
    private final OutOfOrderSequence lastCommittedTx = new ArrayQueueOutOfOrderSequence( -1, 200, new long[1] );
    private final OutOfOrderSequence lastClosedTx = new ArrayQueueOutOfOrderSequence( -1, 200, new long[2] );

    private final int relGrabSize;
    private final CappedOperation<Void> transactionCloseWaitLogger;

    public NeoStore( File fileName, Config conf, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, final LogProvider logProvider,
            RelationshipTypeTokenStore relTypeStore, LabelTokenStore labelTokenStore, PropertyStore propStore,
            RelationshipStore relStore, NodeStore nodeStore, SchemaStore schemaStore,
            RelationshipGroupStore relGroupStore, CountsTracker counts,
            StoreVersionMismatchHandler versionMismatchHandler, Monitors monitors )
    {
        super( fileName, conf, IdType.NEOSTORE_BLOCK, idGeneratorFactory, pageCache, fileSystemAbstraction,
                logProvider, versionMismatchHandler );
        this.relTypeStore = relTypeStore;
        this.labelTokenStore = labelTokenStore;
        this.propStore = propStore;
        this.relStore = relStore;
        this.nodeStore = nodeStore;
        this.schemaStore = schemaStore;
        this.relGroupStore = relGroupStore;
        this.counts = counts;
        this.relGrabSize = conf.get( Configuration.relationship_grab_size );
        this.transactionCloseWaitLogger = new CappedOperation<Void>( time( 30, SECONDS ) )
        {
            @Override
            protected void triggered( Void event )
            {
                log.info( format(
                        "Waiting for all transactions to close...%n committed:  %s%n  committing: %s%n  closed:     %s",
                        lastCommittedTx, lastCommittingTxField, lastClosedTx ) );
            }
        };
        counts.setInitializer( new DataInitializer<CountsAccessor.Updater>()
        {
            @Override
            public void initialize( CountsAccessor.Updater updater )
            {
                log.warn( "Missing counts store, rebuilding it." );
                new CountsComputer( NeoStore.this ).initialize( updater );
            }

            @Override
            public long initialVersion()
            {
                return getLastCommittedTransactionId();
            }
        } );
        try
        {
            counts.init(); // TODO: move this to LifeCycle
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to initialize counts store", e );
        }
    }

    public StoreStatement acquireStatement()
    {
        return new StoreStatement( this );
    }

    @Override
    protected void checkVersion()
    {
        try
        {
            verifyCorrectTypeDescriptorAndVersion();
            /*
             * If the trailing version string check returns normally, either
             * the store is not ok and needs recovery or everything is fine. The
             * latter is boring. The first case however is interesting. If we
             * need recovery we have no idea what the store version is - we erase
             * that information on startup and write it back out on clean shutdown.
             * So, if the above passes and the store is not ok, we check the
             * version field in our store vs the expected one. If it is the same,
             * we can recover and proceed, otherwise we are allowed to die a horrible death.
             */
            if ( !getStoreOk() )
            {
                /*
                 * Could we check that before? Well, yes. But. When we would read in the store version
                 * field it could very well overshoot and read in the version descriptor if the
                 * store is cleanly shutdown. If we are here though the store is not ok, so no
                 * version descriptor so the file is actually smaller than expected so we won't read
                 * in garbage.
                 * Yes, this has to be fixed to be prettier.
                 */
                String foundVersion = versionLongToString( getRecord( pageCache,
                        storageFileName, Position.STORE_VERSION ) );
                if ( !CommonAbstractStore.ALL_STORES_VERSION.equals( foundVersion ) )
                {
                    throw new IllegalStateException(
                            format( "Mismatching store version found (%s while expecting %s). The store cannot be " +
                                    "automatically upgraded since it isn't cleanly shutdown."
                                    + " Recover by starting the database using the previous Neo4j version, " +
                                    "followed by a clean shutdown. Then start with this version again.",
                                    foundVersion, CommonAbstractStore.ALL_STORES_VERSION ) );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to check version " + getStorageFileName(), e );
        }
    }

    /**
     * Closes the node,relationship,property and relationship type stores.
     */
    @Override
    protected void closeStorage()
    {
        if ( relTypeStore != null )
        {
            relTypeStore.close();
            relTypeStore = null;
        }
        if ( labelTokenStore != null )
        {
            labelTokenStore.close();
            labelTokenStore = null;
        }
        if ( propStore != null )
        {
            propStore.close();
            propStore = null;
        }
        if ( relStore != null )
        {
            relStore.close();
            relStore = null;
        }
        if ( nodeStore != null )
        {
            nodeStore.close();
            nodeStore = null;
        }
        if ( schemaStore != null )
        {
            schemaStore.close();
            schemaStore = null;
        }
        if ( relGroupStore != null )
        {
            relGroupStore.close();
            relGroupStore = null;
        }
        if ( counts != null )
        {
            try
            {
                counts.rotate( getLastCommittedTransactionId() );
                counts.shutdown();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
            finally
            {
                counts = null;
            }
        }
    }

    @Override
    public void flush()
    {
        try
        {
            if ( counts != null )
            {
                counts.rotate( getLastCommittedTransactionId() );
            }
            pageCache.flushAndForce();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
        }
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    /**
     * Writes a record in a neostore file.
     * This method only works for neostore files of the current version. It is not guaranteed to correctly handle store
     * version trailers of other store versions.
     *
     * @param pageCache {@link PageCache} the {@code neoStore} file lives in.
     * @param neoStore {@link File} pointing to the neostore.
     * @param position record {@link Position}.
     * @param value value to write in that record.
     * @return the previous value before writing.
     * @throws IOException if any I/O related error occurs.
     */
    public static long setRecord( PageCache pageCache, File neoStore, Position position, long value ) throws IOException
    {
        long previousValue = FIELD_NOT_INITIALIZED;
        try ( PagedFile pagedFile = pageCache.map( neoStore, getPageSize( pageCache ) ) )
        {
            String expectedTrailer = buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );

            long trailerOffset = StoreVersionTrailerUtil.getTrailerOffset( pagedFile, expectedTrailer );
            int recordOffset = RECORD_SIZE * position.id;
            try ( PageCursor pageCursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ) )
            {
                if ( pageCursor.next() )
                {
                    if ( recordOffset < trailerOffset )
                    {
                        // We're overwriting a record, get the previous value
                        long record;
                        byte inUse;
                        do
                        {
                            pageCursor.setOffset( recordOffset );
                            inUse = pageCursor.getByte();
                            record = pageCursor.getLong();
                        }
                        while ( pageCursor.shouldRetry() );
                        if ( inUse == Record.IN_USE.byteValue() )
                        {
                            previousValue = record;
                        }
                    }
                    // Write the value
                    do
                    {
                        pageCursor.setOffset( recordOffset );
                        pageCursor.putByte( Record.IN_USE.byteValue() );
                        pageCursor.putLong( value );
                    }
                    while ( pageCursor.shouldRetry() );
                }
            }

            // Append the trailer if needed
            int newTrailerOffset = recordOffset + RECORD_SIZE;
            if ( newTrailerOffset > trailerOffset )
            {
                StoreVersionTrailerUtil.writeTrailer( pagedFile, UTF8.encode( expectedTrailer ), newTrailerOffset );
            }
        }
        return previousValue;
    }

    /**
     * Reads a record from a neostore file.
     *
     * @param pageCache {@link PageCache} the {@code neoStore} file lives in.
     * @param neoStore {@link File} pointing to the neostore.
     * @param recordPosition record {@link Position}.
     * @return the read record value specified by {@link Position}.
     */
    public static long getRecord( PageCache pageCache, File neoStore, Position recordPosition )
    {
        try ( PagedFile pagedFile = pageCache.map( neoStore, getPageSize( pageCache ) ) )
        {
            if ( pagedFile.getLastPageId() != -1 )
            {
                try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK ) )
                {
                    if ( cursor.next() )
                    {
                        byte recordByte;
                        long record;
                        do
                        {
                            cursor.setOffset( RECORD_SIZE * recordPosition.id );
                            recordByte = cursor.getByte();
                            record = cursor.getLong();
                        }
                        while ( cursor.shouldRetry() );
                        if ( recordByte == Record.IN_USE.byteValue() )
                        {
                            return record;
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return FIELD_NOT_PRESENT;
    }

    private static int getPageSize( PageCache pageCache )
    {
        return pageCache.pageSize() - pageCache.pageSize() % RECORD_SIZE;
    }

    public StoreId getStoreId()
    {
        return new StoreId( getCreationTime(), getRandomNumber(), getUpgradeTime(), upgradeTxIdField );
    }

    public long getUpgradeTime()
    {
        checkInitialized( upgradeTimeField );
        return upgradeTimeField;
    }

    public synchronized void setUpgradeTime( long time )
    {
        setRecord( Position.UPGRADE_TIME, time );
        upgradeTimeField = time;
    }

    public synchronized void setUpgradeTransaction( long id, long checksum )
    {
        setRecord( Position.UPGRADE_TRANSACTION_ID, id );
        upgradeTxIdField = id;
        setRecord( Position.UPGRADE_TRANSACTION_CHECKSUM, checksum );
        upgradeTxChecksumField = checksum;
    }

    public long getCreationTime()
    {
        checkInitialized( creationTimeField );
        return creationTimeField;
    }

    public synchronized void setCreationTime( long time )
    {
        setRecord( Position.TIME, time );
        creationTimeField = time;
    }

    public long getRandomNumber()
    {
        checkInitialized( randomNumberField );
        return randomNumberField;
    }

    public synchronized void setRandomNumber( long nr )
    {
        setRecord( Position.RANDOM_NUMBER, nr );
        randomNumberField = nr;
    }

    @Override
    public long getCurrentLogVersion()
    {
        checkInitialized( versionField );
        return versionField;
    }

    public void setCurrentLogVersion( long version )
    {
        setRecord( Position.LOG_VERSION, version );
        versionField = version;
    }

    @Override
    public long incrementAndGetVersion()
    {
        // This method can expect synchronisation at a higher level,
        // and be effectively single-threaded.
        // The call to getVersion() will most likely optimise to a volatile-read.
        long pageId = pageIdForRecord( Position.LOG_VERSION.id );
        try ( PageCursor cursor = storeFile.io( pageId, PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() )
            {
                incrementVersion( cursor );
            }
            return versionField;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        finally
        {
            try
            {
                // make sure the new version value is persisted
                // TODO this can be improved by flushing only the page containing that value rather than all pages
                storeFile.flushAndForce();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
    }

    public long getStoreVersion()
    {
        checkInitialized( storeVersionField );
        return storeVersionField;
    }

    public void setStoreVersion( long version )
    {
        setRecord( Position.STORE_VERSION, version );
        storeVersionField = version;
    }

    public long getGraphNextProp()
    {
        checkInitialized( graphNextPropField );
        return graphNextPropField;
    }

    public void setGraphNextProp( long propId )
    {
        setRecord( Position.FIRST_GRAPH_PROPERTY, propId );
        graphNextPropField = propId;
    }

    public long getLatestConstraintIntroducingTx()
    {
        checkInitialized( latestConstraintIntroducingTxField );
        return latestConstraintIntroducingTxField;
    }

    public void setLatestConstraintIntroducingTx( long latestConstraintIntroducingTx )
    {
        setRecord( Position.LAST_CONSTRAINT_TRANSACTION, latestConstraintIntroducingTx );
        latestConstraintIntroducingTxField = latestConstraintIntroducingTx;
    }

    private void readAllFields( PageCursor cursor ) throws IOException
    {
        do
        {
            creationTimeField = getRecordValue( cursor, Position.TIME );
            randomNumberField = getRecordValue( cursor, Position.RANDOM_NUMBER );
            versionField = getRecordValue( cursor, Position.LOG_VERSION );
            upgradeTxIdField = getRecordValue( cursor, Position.UPGRADE_TRANSACTION_ID );
            upgradeTimeField = getRecordValue( cursor, Position.UPGRADE_TIME );
            long lastCommittedTxId = getRecordValue( cursor, Position.LAST_TRANSACTION_ID );
            lastCommittingTxField.set( lastCommittedTxId );
            storeVersionField = getRecordValue( cursor, Position.STORE_VERSION );
            graphNextPropField = getRecordValue( cursor, Position.FIRST_GRAPH_PROPERTY );
            latestConstraintIntroducingTxField = getRecordValue( cursor, Position.LAST_CONSTRAINT_TRANSACTION );
            lastTransactionChecksum = getRecordValue( cursor, Position.LAST_TRANSACTION_CHECKSUM );
            lastClosedTransactionLogVersion = getRecordValue( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
            lastClosedTransactionLogByteOffset = getRecordValue( cursor, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
            lastClosedTx.set( lastCommittedTxId,
                    new long[]{lastClosedTransactionLogVersion, lastClosedTransactionLogByteOffset} );
            lastCommittedTx.set( lastCommittedTxId, new long[]{lastTransactionChecksum} );
            upgradeTxChecksumField = getRecordValue( cursor, Position.UPGRADE_TRANSACTION_CHECKSUM );
        }
        while ( cursor.shouldRetry() );
    }

    private long getRecordValue( PageCursor cursor, Position position )
    {
        int offset = position.id * getRecordSize();
        cursor.setOffset( offset );
        if ( cursor.getByte() == Record.IN_USE.byteValue() )
        {
            return cursor.getLong();
        }
        return FIELD_NOT_PRESENT;
    }

    private void incrementVersion( PageCursor cursor ) throws IOException
    {
        int offset = Position.LOG_VERSION.id * getRecordSize();
        long value;
        do
        {
            cursor.setOffset( offset + 1 ); // +1 to skip the inUse byte
            value = cursor.getLong() + 1;
            cursor.setOffset( offset + 1 ); // +1 to skip the inUse byte
            cursor.putLong( value );
        }
        while ( cursor.shouldRetry() );
        versionField = value;
    }

    private void refreshFields()
    {
        scanAllFields( PF_SHARED_LOCK );
    }

    private void scanAllFields( int pf_flags )
    {
        try ( PageCursor cursor = storeFile.io( 0, pf_flags ) )
        {
            if ( cursor.next() )
            {
                readAllFields( cursor );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void setRecord( Position recordPosition, long value )
    {
        long id = recordPosition.id;
        long pageId = pageIdForRecord( id );

        // We need to do a little special handling of high id in neostore since it's not updated in the same
        // way as other stores. Other stores always gets updates via commands where records are updated and
        // the one making the update can also track the high id in the event of recovery.
        // Here methods can be called directly, for example setLatestConstraintIntroducingTx where it's
        // unclear from the outside which record id that refers to, so here we need to manage high id ourselves.
        setHighestPossibleIdInUse( id );

        try ( PageCursor cursor = storeFile.io( pageId, PF_EXCLUSIVE_LOCK ) )
        {
            if ( cursor.next() )
            {
                int offset = offsetForId( id );
                do
                {
                    cursor.setOffset( offset );
                    cursor.putByte( Record.IN_USE.byteValue() );
                    cursor.putLong( value );
                }
                while ( cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Returns the node store.
     *
     * @return The node store
     */
    public NodeStore getNodeStore()
    {
        return nodeStore;
    }

    /**
     * @return the schema store.
     */
    public SchemaStore getSchemaStore()
    {
        return schemaStore;
    }

    /**
     * The relationship store.
     *
     * @return The relationship store
     */
    public RelationshipStore getRelationshipStore()
    {
        return relStore;
    }

    /**
     * Returns the relationship type store.
     *
     * @return The relationship type store
     */
    public RelationshipTypeTokenStore getRelationshipTypeTokenStore()
    {
        return relTypeStore;
    }

    /**
     * Returns the label store.
     *
     * @return The label store
     */
    public LabelTokenStore getLabelTokenStore()
    {
        return labelTokenStore;
    }

    /**
     * Returns the property store.
     *
     * @return The property store
     */
    public PropertyStore getPropertyStore()
    {
        return propStore;
    }

    /**
     * @return the {@link PropertyKeyTokenStore}
     */
    public PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return propStore.getPropertyKeyTokenStore();
    }

    /**
     * @return the {@link RelationshipGroupStore}
     */
    public RelationshipGroupStore getRelationshipGroupStore()
    {
        return relGroupStore;
    }

    public CountsTracker getCounts()
    {
        return counts;
    }

    @Override
    public void makeStoreOk()
    {
        relTypeStore.makeStoreOk();
        labelTokenStore.makeStoreOk();
        propStore.makeStoreOk();
        relStore.makeStoreOk();
        nodeStore.makeStoreOk();
        schemaStore.makeStoreOk();
        relGroupStore.makeStoreOk();
        super.makeStoreOk();
    }

    public void rebuildIdGenerators()
    {
        relTypeStore.rebuildIdGenerator();
        labelTokenStore.rebuildIdGenerator();
        propStore.rebuildIdGenerator();
        relStore.rebuildIdGenerator();
        nodeStore.rebuildIdGenerator();
        schemaStore.rebuildIdGenerator();
        relGroupStore.rebuildIdGenerator();
        super.rebuildIdGenerator();
    }

    public int getRelationshipGrabSize()
    {
        return relGrabSize;
    }

    /**
     * Throws cause of store not being OK.
     */
    public void verifyStoreOk()
    {
        visitStore( new Visitor<CommonAbstractStore,RuntimeException>()
        {
            @Override
            public boolean visit( CommonAbstractStore element )
            {
                element.checkStoreOk();
                return false;
            }
        } );
    }

    @Override
    public void logVersions( Logger msgLog )
    {
        msgLog.log( "Store versions:" );
        super.logVersions( msgLog );
        schemaStore.logVersions( msgLog );
        nodeStore.logVersions( msgLog );
        relStore.logVersions( msgLog );
        relTypeStore.logVersions( msgLog );
        labelTokenStore.logVersions( msgLog );
        propStore.logVersions( msgLog );
        relGroupStore.logVersions( msgLog );
    }

    @Override
    public void logIdUsage( Logger msgLog )
    {
        msgLog.log( "Id usage:" );
        schemaStore.logIdUsage( msgLog );
        nodeStore.logIdUsage( msgLog );
        relStore.logIdUsage( msgLog );
        relTypeStore.logIdUsage( msgLog );
        labelTokenStore.logIdUsage( msgLog );
        propStore.logIdUsage( msgLog );
        relGroupStore.logIdUsage( msgLog );
    }

    public NeoStoreRecord asRecord()
    {
        NeoStoreRecord result = new NeoStoreRecord();
        result.setNextProp( getGraphNextProp() );
        return result;
    }

    /*
     * The following two methods encode and decode a string that is presumably
     * the store version into a long via Latin1 encoding. This leaves room for
     * 7 characters and 1 byte for the length. Current string is
     * 0.A.0 which is 5 chars, so we have room for expansion. When that
     * becomes a problem we will be in a yacht, sipping alcoholic
     * beverages of our choice. Or taking turns crashing golden
     * helicopters. Anyway, it should suffice for some time and by then
     * it should have become SEP.
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
            throw new IllegalArgumentException( String.format(
                    "The given string %s is not of proper size for a store version string", storeVersion ) );
        }
        bits.put( length, 8 );
        for ( int i = 0; i < length; i++ )
        {
            char c = storeVersion.charAt( i );
            if ( c < 0 || c >= 256 )
            {
                throw new IllegalArgumentException( String.format(
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
            throw new IllegalArgumentException( String.format( "The read version string length %d is not proper.",
                    length ) );
        }
        char[] result = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (char) bits.getShort( 8 );
        }
        return new String( result );
    }

    public int getDenseNodeThreshold()
    {
        return getRelationshipGroupStore().getDenseNodeThreshold();
    }

    @Override
    public long nextCommittingTransactionId()
    {
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittingTxField.incrementAndGet();
    }

    @Override
    public void transactionCommitted( long transactionId, long checksum )
    {
        if ( lastCommittedTx.offer( transactionId, new long[]{checksum} ) )
        {
            long[] transactionData = lastCommittedTx.get();
            setRecord( Position.LAST_TRANSACTION_ID, transactionData[0] );
            setRecord( Position.LAST_TRANSACTION_CHECKSUM, transactionData[1] );
            lastTransactionChecksum = checksum;
        }
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittedTx.getHighestGapFreeNumber();
    }

    @Override
    public long[] getLastCommittedTransaction()
    {
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittedTx.get();
    }

    @Override
    public long[] getUpgradeTransaction()
    {
        checkInitialized( upgradeTxChecksumField );
        return new long[]{upgradeTxIdField, upgradeTxChecksumField};
    }

    @Override
    public long getLastClosedTransactionId()
    {
        checkInitialized( lastCommittingTxField.get() );
        return lastClosedTx.getHighestGapFreeNumber();
    }

    @Override
    public long[] getLastClosedTransaction()
    {
        checkInitialized( lastCommittingTxField.get() );
        return lastClosedTx.get();
    }

    // Ensures that all fields are read from the store, by checking the initial value of the field in question
    private void checkInitialized( long field )
    {
        if ( field == FIELD_NOT_INITIALIZED )
        {
            refreshFields();
        }
    }

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId, long checksum, long logVersion, long byteOffset )
    {
        setRecord( Position.LAST_TRANSACTION_ID, transactionId );
        setRecord( Position.LAST_TRANSACTION_CHECKSUM, checksum );
        setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, logVersion );
        setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, byteOffset );
        checkInitialized( lastCommittingTxField.get() );
        lastCommittingTxField.set( transactionId );
        lastCommittedTx.set( transactionId, new long[]{checksum} );
        lastTransactionChecksum = checksum;
        lastClosedTx.set( transactionId, new long[]{logVersion, byteOffset} );
        lastClosedTransactionLogVersion = logVersion;
        lastClosedTransactionLogByteOffset = byteOffset;
    }

    @Override
    public void transactionClosed( long transactionId, long logVersion, long byteOffset )
    {
        if ( lastClosedTx.offer( transactionId, new long[]{logVersion, byteOffset} ) )
        {
            long[] lastClosedTransactionData = lastClosedTx.get();
            setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, lastClosedTransactionData[1] );
            setRecord( Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, lastClosedTransactionData[2] );
            lastClosedTransactionLogVersion = lastClosedTransactionData[1];
            lastClosedTransactionLogByteOffset = lastClosedTransactionData[2];
        }
    }

    @Override
    public boolean closedTransactionIdIsOnParWithOpenedTransactionId()
    {
        boolean onPar = lastClosedTx.getHighestGapFreeNumber() == lastCommittingTxField.get();
        if ( !onPar )
        {   // Trigger some logging here, max logged every 30 secs or so
            transactionCloseWaitLogger.event( null );
        }
        return onPar;
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #makeStoreOk()},
     * {@link #closeStorage()} (where that method could be deleted all together and do a visit in {@link #close()}),
     * {@link #logIdUsage(Logger)},
     * {@link #logVersions(Logger)},
     * For a good samaritan to pick up later.
     */
    @Override
    public void visitStore( Visitor<CommonAbstractStore,RuntimeException> visitor )
    {
        nodeStore.visitStore( visitor );
        relStore.visitStore( visitor );
        relGroupStore.visitStore( visitor );
        relTypeStore.visitStore( visitor );
        labelTokenStore.visitStore( visitor );
        propStore.visitStore( visitor );
        schemaStore.visitStore( visitor );
        visitor.visit( this );
    }

    public void rebuildCountStoreIfNeeded() throws IOException
    {
        // TODO: move this to LifeCycle
        counts.start();
    }

    public void assertOpen()
    {
        if ( nodeStore == null )
        {
            throw new IllegalStateException( "Database has been shutdown" );
        }
    }
}
