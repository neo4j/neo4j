/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.OutOfOrderSequence;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

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
        public static final Setting<Integer> dense_node_threshold = GraphDatabaseSettings.dense_node_threshold;
    }

    public static final String TYPE_DESCRIPTOR = "NeoStore";
    // This value means the field has not been refreshed from the store. Normally, this should happen only once
    public static final long FIELD_NOT_INITIALIZED = Long.MIN_VALUE;
    /*
     *  9 longs in header (long + in use), time | random | version | txid | store version | graph next prop | latest
     *  constraint tx | upgrade time | upgrade id
     */
    public static final int RECORD_SIZE = 9;
    public static final String DEFAULT_NAME = "neostore";
    // Positions of meta-data records

    public static final int TIME_POSITION = 0;
    public static final int RANDOM_NUMBER_POSITION = 1;
    public static final int VERSION_POSITION = 2;
    public static final int LATEST_TX_POSITION = 3;
    public static final int STORE_VERSION_POSITION = 4;
    public static final int NEXT_GRAPH_PROP_POSITION = 5;
    public static final int LATEST_CONSTRAINT_TX_POSITION = 6;
    public static final int UPGRADE_ID_POSITION = 7;
    public static final int UPGRADE_TIME_POSITION = 8;
    public static final int META_DATA_RECORD_COUNT = UPGRADE_TIME_POSITION + 1;
    // NOTE: When adding new constants, remember to update the fields bellow,
    // and the apply() method!

    public static boolean isStorePresent( FileSystemAbstraction fs, Config config )
    {
        File neoStore = config.get( org.neo4j.kernel.impl.store.CommonAbstractStore.Configuration.neo_store );
        return fs.fileExists( neoStore );
    }

    private NodeStore nodeStore;
    private PropertyStore propStore;
    private RelationshipStore relStore;
    private RelationshipTypeTokenStore relTypeStore;
    private LabelTokenStore labelTokenStore;
    private SchemaStore schemaStore;
    private RelationshipGroupStore relGroupStore;
    private CountsStore countsStore;

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
    private volatile long upgradeIdField = FIELD_NOT_INITIALIZED;
    private volatile long upgradeTimeField = FIELD_NOT_INITIALIZED;

    // This is not a field in the store, but something keeping track of which of the committed
    // transactions have been closed. Useful in rotation and shutdown.
    private final OutOfOrderSequence lastCommittedTx = new ArrayQueueOutOfOrderSequence( -1, 200 );
    private final OutOfOrderSequence lastClosedTx = new ArrayQueueOutOfOrderSequence( -1, 200 );

    private final int relGrabSize;
    private final CappedOperation<Void> transactionCloseWaitLogger;

    public NeoStore( File fileName, Config conf, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
                     FileSystemAbstraction fileSystemAbstraction, final StringLogger stringLogger,
                     RelationshipTypeTokenStore relTypeStore, LabelTokenStore labelTokenStore, PropertyStore propStore,
                     RelationshipStore relStore, NodeStore nodeStore, SchemaStore schemaStore,
                     RelationshipGroupStore relGroupStore, CountsStore countsStore,
                     StoreVersionMismatchHandler versionMismatchHandler, Monitors monitors )
    {
        super( fileName, conf, IdType.NEOSTORE_BLOCK, idGeneratorFactory, pageCache, fileSystemAbstraction,
                stringLogger, versionMismatchHandler, monitors );
        this.relTypeStore = relTypeStore;
        this.labelTokenStore = labelTokenStore;
        this.propStore = propStore;
        this.relStore = relStore;
        this.nodeStore = nodeStore;
        this.schemaStore = schemaStore;
        this.relGroupStore = relGroupStore;
        this.countsStore = countsStore;
        this.relGrabSize = conf.get( Configuration.relationship_grab_size );
        this.transactionCloseWaitLogger = new CappedOperation<Void>( time( 30, SECONDS ) )
        {
            @Override
            protected void triggered( Void event )
            {
                stringLogger.info( format( "Waiting for all transactions to close...%n  committed: %s%n  closed:    %s",
                        lastCommittedTx, lastClosedTx ) );
            }
        };
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
                String foundVersion = versionLongToString( getStoreVersion( fileSystemAbstraction, configuration
                        .get( org.neo4j.kernel.impl.store.CommonAbstractStore.Configuration.neo_store ) ) );
                if ( !CommonAbstractStore.ALL_STORES_VERSION.equals( foundVersion ) )
                {
                    throw new IllegalStateException(
                            format( "Mismatching store version found (%s while expecting %s). The store cannot be automatically upgraded since it isn't cleanly shutdown."
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
        if ( countsStore != null )
        {
            countsStore.close();
            countsStore = null;
        }
    }

    @Override
    public void flush()
    {
        try
        {
            pageCache.flush();
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
     * Sets the version for the given {@code neoStore} file.
     * @param neoStore the NeoStore file.
     * @param version the version to set.
     * @return the previous version before writing.
     */
    public static long setVersion( FileSystemAbstraction fileSystem, File neoStore, long version )
    {
        return setRecord( fileSystem, neoStore, VERSION_POSITION, version );
    }

    /**
     * Sets the store version for the given {@code neoStore} file.
     * @param neoStore the NeoStore file.
     * @param storeVersion the version to set.
     * @return the previous version before writing.
     */
    public static long setStoreVersion( FileSystemAbstraction fileSystem, File neoStore, long storeVersion )
    {
        return setRecord( fileSystem, neoStore, STORE_VERSION_POSITION, storeVersion );
    }

    /**
     * Sets the upgrade id for the given {@code neoStore} file.
     *
     * @param neoStore the NeoStore file.
     * @param id       the version to set.
     * @return the previous version before writing.
     */
    public static long setOrAddUpgradeIdOnMigration( FileSystemAbstraction fileSystem, File neoStore, long id )
    {
        return setRecord( fileSystem, neoStore, UPGRADE_ID_POSITION, id );
    }

    /**
     * Sets the upgrade time for the given {@code neoStore} file.
     *
     * @param neoStore the NeoStore file.
     * @param time     the version to set.
     * @return the previous version before writing.
     */
    public static long setOrAddUpgradeTimeOnMigration( FileSystemAbstraction fileSystem, File neoStore, long time )
    {
        return setRecord( fileSystem, neoStore, UPGRADE_TIME_POSITION, time );
    }

    private static long setRecord( FileSystemAbstraction fileSystem, File neoStore, int position, long value )
    {
        int trailerSize = UTF8.encode( buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ) ).length;
        try ( StoreChannel channel = fileSystem.open( neoStore, "rw" ) )
        {
            long previous = FIELD_NOT_INITIALIZED;

            long actualFieldsSize = channel.size() - trailerSize;
            ByteBuffer buffer = ByteBuffer.allocate( RECORD_SIZE - 1/*inUse*/ );
            ByteBuffer trailerBuffer = null;
            int recordOffset = RECORD_SIZE * position + 1/*inUse*/;
            if ( recordOffset < actualFieldsSize )
            {
                channel.position( recordOffset );
                channel.read( buffer );
                buffer.flip();
                previous = buffer.getLong();
            }
            else
            {
                trailerBuffer = ByteBuffer.allocate( trailerSize );
                channel.position( actualFieldsSize );
                channel.read( trailerBuffer );
                trailerBuffer.flip();
                channel.truncate( actualFieldsSize );
            }
            buffer.clear();

            channel.position( recordOffset );
            buffer.putLong( value );
            buffer.flip();
            channel.write( buffer );

            int afterRecordSize = recordOffset + RECORD_SIZE - 1/*inUse*/;
            if ( afterRecordSize > actualFieldsSize )
            {
                assert trailerBuffer != null;
                channel.position( afterRecordSize );
                channel.write( trailerBuffer );
            }
            return previous;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Warning: This method only works for stores where there is no database running!
     */
    public static long getStoreVersion( FileSystemAbstraction fs, File neoStore )
    {
        return getRecord( fs, neoStore, STORE_VERSION_POSITION );
    }

    /**
     * Warning: This method only works for stores where there is no database running!
     */
    public static long getTxId( FileSystemAbstraction fs, File neoStore )
    {
        return getRecord( fs, neoStore, LATEST_TX_POSITION );
    }

    private static long getRecord( FileSystemAbstraction fs, File neoStore, int recordPosition )
    {
        try ( StoreChannel channel = fs.open( neoStore, "r" ) )
        {
            /*
             * We have to check size, because the store version
             * field was introduced with 1.5, so if there is a non-clean
             * shutdown we may have a buffer underflow.
             */
            if ( recordPosition > 3 && channel.size() < RECORD_SIZE * 5 )
            {
                return -1;
            }
            channel.position( RECORD_SIZE * recordPosition + 1/*inUse*/);
            ByteBuffer buffer = ByteBuffer.allocate( 8 );
            channel.read( buffer );
            buffer.flip();
            return buffer.getLong();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public StoreId getStoreId()
    {
        return new StoreId( getCreationTime(), getRandomNumber(), getUpgradeTime(), getUpgradeId() );
    }

    public long getUpgradeTime()
    {
        checkInitialized( upgradeTimeField );
        return upgradeTimeField;
    }

    public synchronized void setUpgradeTime( long time )
    {
        setRecord( UPGRADE_TIME_POSITION, time );
        upgradeTimeField = time;
    }

    public long getUpgradeId()
    {
        checkInitialized( upgradeIdField );
        return upgradeIdField;
    }

    public synchronized void setUpgradeId( long id )
    {
        setRecord( UPGRADE_ID_POSITION, id );
        upgradeIdField = id;
    }

    public long getCreationTime()
    {
        checkInitialized( creationTimeField );
        return creationTimeField;
    }

    public synchronized void setCreationTime( long time )
    {
        setRecord( TIME_POSITION, time );
        creationTimeField = time;
    }

    public long getRandomNumber()
    {
        checkInitialized( randomNumberField );
        return randomNumberField;
    }

    public synchronized void setRandomNumber( long nr )
    {
        setRecord( RANDOM_NUMBER_POSITION, nr );
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
        setRecord( VERSION_POSITION, version );
        versionField = version;
    }

    @Override
    public long incrementAndGetVersion()
    {
        // This method can expect synchronisation at a higher level,
        // and be effectively single-threaded.
        // The call to getVersion() will most likely optimise to a volatile-read.
        long pageId = pageIdForRecord( VERSION_POSITION );
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
                storeFile.flush();
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
        setRecord( STORE_VERSION_POSITION, version );
        storeVersionField = version;
    }

    public long getGraphNextProp()
    {
        checkInitialized( graphNextPropField );
        return graphNextPropField;
    }

    public void setGraphNextProp( long propId )
    {
        setRecord( NEXT_GRAPH_PROP_POSITION, propId );
        graphNextPropField = propId;
    }

    public long getLatestConstraintIntroducingTx()
    {
        checkInitialized( latestConstraintIntroducingTxField );
        return latestConstraintIntroducingTxField;
    }

    public void setLatestConstraintIntroducingTx( long latestConstraintIntroducingTx )
    {
        setRecord( LATEST_CONSTRAINT_TX_POSITION, latestConstraintIntroducingTx );
        latestConstraintIntroducingTxField = latestConstraintIntroducingTx;
    }

    private void readAllFields( PageCursor cursor ) throws IOException
    {
        do
        {
            creationTimeField = getRecordValue( cursor, TIME_POSITION );
            randomNumberField = getRecordValue( cursor, RANDOM_NUMBER_POSITION );
            versionField = getRecordValue( cursor, VERSION_POSITION );
            upgradeIdField = getRecordValue( cursor, UPGRADE_ID_POSITION );
            upgradeTimeField = getRecordValue( cursor, UPGRADE_TIME_POSITION );
            long lastCommittedTxId = getRecordValue( cursor, LATEST_TX_POSITION );
            lastCommittingTxField.set( lastCommittedTxId );
            storeVersionField = getRecordValue( cursor, STORE_VERSION_POSITION );
            graphNextPropField = getRecordValue( cursor, NEXT_GRAPH_PROP_POSITION );
            latestConstraintIntroducingTxField = getRecordValue( cursor, LATEST_CONSTRAINT_TX_POSITION );
            lastClosedTx.set( lastCommittedTxId );
            lastCommittedTx.set( lastCommittedTxId );
        } while ( cursor.shouldRetry() );
    }

    private long getRecordValue( PageCursor cursor, int position )
    {
        // The "+ 1" to skip over the inUse byte.
        int offset = position * getEffectiveRecordSize() + 1;
        cursor.setOffset( offset );
        return cursor.getLong();
    }

    private void incrementVersion( PageCursor cursor ) throws IOException
    {
        int offset = VERSION_POSITION * getEffectiveRecordSize();
        long value;
        do
        {
            cursor.setOffset( offset + 1 ); // +1 to skip the inUse byte
            value = cursor.getLong() + 1;
            cursor.setOffset( offset + 1 ); // +1 to skip the inUse byte
            cursor.putLong( value );
        } while ( cursor.shouldRetry() );
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

    private void setRecord( long id, long value )
    {
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
                } while ( cursor.shouldRetry() );
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

    public CountsStore getCountsStore()
    {
        return countsStore;
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

    public int getRelationshipGrabSize()
    {
        return relGrabSize;
    }

    public boolean isStoreOk()
    {
        return getStoreOk() && relTypeStore.getStoreOk() && labelTokenStore.getStoreOk() && propStore.getStoreOk()
                && relStore.getStoreOk() && nodeStore.getStoreOk() && schemaStore.getStoreOk()
                && relGroupStore.getStoreOk();
    }

    @Override
    public void logVersions( StringLogger.LineLogger msgLog )
    {
        msgLog.logLine( "Store versions:" );
        super.logVersions( msgLog );
        schemaStore.logVersions( msgLog );
        nodeStore.logVersions( msgLog );
        relStore.logVersions( msgLog );
        relTypeStore.logVersions( msgLog );
        labelTokenStore.logVersions( msgLog );
        propStore.logVersions( msgLog );
        relGroupStore.logVersions( msgLog );
        stringLogger.flush();
    }

    @Override
    public void logIdUsage( StringLogger.LineLogger msgLog )
    {
        msgLog.logLine( "Id usage:" );
        schemaStore.logIdUsage( msgLog );
        nodeStore.logIdUsage( msgLog );
        relStore.logIdUsage( msgLog );
        relTypeStore.logIdUsage( msgLog );
        labelTokenStore.logIdUsage( msgLog );
        propStore.logIdUsage( msgLog );
        relGroupStore.logIdUsage( msgLog );
        stringLogger.flush();
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
        Bits bits = Bits.bitsFromLongs( new long[] { storeVersion } );
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
    public void transactionCommitted( long transactionId )
    {
        long previous = lastCommittedTx.get();
        lastCommittedTx.offer( transactionId );
        if ( transactionId > previous )
        {
            /*
             * TODO 2.2-future
             * In order to make pull updates to work I need to write this record everytime it is updated.
             * Probably there are better ways to handle this, not sut though since it looks like slaves never
             * flush the store so this record is never written to disk causing problem in sync when the old master
             * becomes available again. In particular all the changes committed on the slaves are lost.
             */
            setRecord( LATEST_TX_POSITION, lastCommittedTx.get() );
        }
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        checkInitialized( lastCommittingTxField.get() );
        return lastCommittedTx.get();
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
    public void setLastCommittedAndClosedTransactionId( long transactionId )
    {
        /*
         * TODO 2.2-future
         * In order to make pull updates to work I need to write this record everytime it is updated.
         * Probably there are better ways to handle this, not sut though since it looks like slaves never
         * flush the store so this record is never written to disk causing problem in sync when the old master
         * becomes available again. In particular all the changes committed on the slaves are lost.
         */
        setRecord( LATEST_TX_POSITION, transactionId );
        checkInitialized( lastCommittingTxField.get() );
        lastCommittingTxField.set( transactionId );
        lastClosedTx.set( transactionId );
        lastCommittedTx.set( transactionId );
    }

    @Override
    public void transactionClosed( long transactionId )
    {
        lastClosedTx.offer( transactionId );
    }

    @Override
    public boolean closedTransactionIdIsOnParWithCommittedTransactionId()
    {
        boolean onPar = lastClosedTx.get() == lastCommittedTx.get();
        if ( !onPar )
        {   // Trigger some logging here, max logged every 30 secs or so
            transactionCloseWaitLogger.event( null );
        }
        return onPar;
    }
}
