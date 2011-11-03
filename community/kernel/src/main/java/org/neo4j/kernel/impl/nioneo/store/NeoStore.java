/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.DatabaseFiles;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStore doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" validation
 * performed in there.
 */
public class NeoStore extends AbstractStore
{
    public static final String TYPE_DESCRIPTOR = "NeoStore";

    /*
     *  5 longs in header (long + in use), time | random | version | txid | store version
     */
    private static final int RECORD_SIZE = 9;
    private static final int DEFAULT_REL_GRAB_SIZE = 100;

    public static final String DEFAULT_NAME = "neostore";

    private NodeStore nodeStore;
    private PropertyStore propStore;
    private RelationshipStore relStore;
    private RelationshipTypeStore relTypeStore;
    private final LastCommittedTxIdSetter lastCommittedTxIdSetter;
    private final IdGeneratorFactory idGeneratorFactory;
    private final TxHook txHook;
    private boolean isStarted;
    private long lastCommittedTx = -1;

    private final int REL_GRAB_SIZE;

    public NeoStore( Map<?,?> config )
    {
        super( (String) config.get( "neo_store" ), config, IdType.NEOSTORE_BLOCK );
        int relGrabSize = DEFAULT_REL_GRAB_SIZE;
        if ( getConfig() != null )
        {
            String grabSize = (String) getConfig().get( "relationship_grab_size" );
            if ( grabSize != null )
            {
                relGrabSize = Integer.parseInt( grabSize );
            }
        }
        REL_GRAB_SIZE = relGrabSize;
        lastCommittedTxIdSetter = (LastCommittedTxIdSetter)
                config.get( LastCommittedTxIdSetter.class );
        idGeneratorFactory = (IdGeneratorFactory) config.get( IdGeneratorFactory.class );
        txHook = (TxHook) config.get( TxHook.class );
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
                String foundVersion = versionLongToString( getStoreVersion( (String) getConfig().get(
                        "neo_store" ) ) );
                if ( !CommonAbstractStore.ALL_STORES_VERSION.equals( foundVersion ) )
                {
                    throw new IllegalStateException(
                            String.format(
                                    "Mismatching store version found (%s while expecting %s) and the store is not cleanly shutdown."
                                            + " Recover the database with the previous database version and then attempt to upgrade",
                                    foundVersion,
                                    CommonAbstractStore.ALL_STORES_VERSION ) );
                }
            }
        }
        catch ( NotCurrentStoreVersionException e )
        {
            releaseFileLockAndCloseFileChannel();
            tryToUpgradeStores();
            checkStorage();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to check version "
                    + getStorageFileName(), e );
        }
    }

    @Override
    protected void initStorage()
    {
        instantiateChildStores();
    }

    /**
     * Initializes the node,relationship,property and relationship type stores.
     */
    private void instantiateChildStores()
    {
        relTypeStore = new RelationshipTypeStore( getStorageFileName()
        + ".relationshiptypestore.db", getConfig(), IdType.RELATIONSHIP_TYPE );
        propStore = new PropertyStore( getStorageFileName()
        + ".propertystore.db", getConfig() );
        relStore = new RelationshipStore( getStorageFileName()
        + ".relationshipstore.db", getConfig() );
        nodeStore = new NodeStore( getStorageFileName() + ".nodestore.db",
        getConfig() );
    }

    private void tryToUpgradeStores()
    {
        new StoreUpgrader( getConfig(), new ConfigMapUpgradeConfiguration(getConfig()),
                new UpgradableDatabase(), new StoreMigrator( new VisibleMigrationProgressMonitor( System.out ) ),
                new DatabaseFiles() ).attemptUpgrade( getStorageFileName() );
    }

    /**
     * Closes the node,relationship,property and relationship type stores.
     */
    @Override
    protected void closeStorage()
    {
        if ( lastCommittedTxIdSetter != null ) lastCommittedTxIdSetter.close();
        if ( relTypeStore != null )
        {
            relTypeStore.close();
            relTypeStore = null;
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
    }

    @Override
    public void flushAll()
    {
        if ( relTypeStore == null || propStore == null || relStore == null ||
                nodeStore == null )
        {
            return;
        }
        relTypeStore.flushAll();
        propStore.flushAll();
        relStore.flushAll();
        nodeStore.flushAll();
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }
    
    public TxHook getTxHook()
    {
        return txHook;
    }

    /**
     * Creates the neo,node,relationship,property and relationship type stores.
     *
     * @param fileName
     *            The name of store
     * @param config
     *            Map of configuration parameters
     */
    public static void createStore( String fileName, Map<?,?> config )
    {
        IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                IdGeneratorFactory.class );
        StoreId storeId = (StoreId) config.get( StoreId.class );
        if ( storeId == null ) storeId = new StoreId();

        createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ), idGeneratorFactory );
        NodeStore.createStore( fileName + ".nodestore.db", config );
        RelationshipStore.createStore( fileName + ".relationshipstore.db", idGeneratorFactory );
        PropertyStore.createStore( fileName + ".propertystore.db", config );
        RelationshipTypeStore.createStore( fileName
            + ".relationshiptypestore.db", config );
        if ( !config.containsKey( "neo_store" ) )
        {
            // TODO Ugly
            Map<Object, Object> newConfig = new HashMap<Object, Object>( config );
            newConfig.put( "neo_store", fileName );
            config = newConfig;
        }
        NeoStore neoStore = new NeoStore( config );
        /*
         *  created time | random long | backup version | tx id | store version
         */
        neoStore.nextId();
        neoStore.nextId();
        neoStore.nextId();
        neoStore.nextId();
        neoStore.nextId();
        neoStore.setCreationTime( storeId.getCreationTime() );
        neoStore.setRandomNumber( storeId.getRandomId() );
        neoStore.setVersion( 0 );
        neoStore.setLastCommittedTx( 1 );
        neoStore.setStoreVersion( storeId.getStoreVersion() );
        neoStore.close();
    }

    /**
     * Sets the version for the given neostore file in {@code storeDir}.
     * @param storeDir the store dir to locate the neostore file in.
     * @param version the version to set.
     * @return the previous version before writing.
     */
    public static long setVersion( String storeDir, long version )
    {
        RandomAccessFile file = null;
        try
        {
            file = new RandomAccessFile( new File( storeDir, NeoStore.DEFAULT_NAME ), "rw" );
            FileChannel channel = file.getChannel();
            channel.position( RECORD_SIZE*2+1/*inUse*/ );
            ByteBuffer buffer = ByteBuffer.allocate( 8 );
            channel.read( buffer );
            buffer.flip();
            long previous = buffer.getLong();
            channel.position( RECORD_SIZE*2+1/*inUse*/ );
            buffer.clear();
            buffer.putLong( version ).flip();
            channel.write( buffer );
            return previous;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            try
            {
                if ( file != null ) file.close();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    public static long getStoreVersion( String storeDir )
    {
        /*
         * We have to check size, because the store version
         * field was introduced with 1.5, so if there is a non-clean
         * shutdown we may have a buffer underflow.
         */
        RandomAccessFile file = null;
        try
        {
            file = new RandomAccessFile( new File( storeDir ), "rw" );
            FileChannel channel = file.getChannel();
            if ( channel.size() < RECORD_SIZE * 5 )
            {
                return -1;
            }
            channel.position( RECORD_SIZE * 4 + 1/*inUse*/);
            ByteBuffer buffer = ByteBuffer.allocate( 8 );
            channel.read( buffer );
            buffer.flip();
            long previous = buffer.getLong();
            return previous;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            try
            {
                if ( file != null ) file.close();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    public StoreId getStoreId()
    {
        return new StoreId( getCreationTime(), getRandomNumber(),
                getStoreVersion() );
    }

    public long getCreationTime()
    {
        return getRecord( 0 );
    }

    public void setCreationTime( long time )
    {
        setRecord( 0, time );
    }

    public long getRandomNumber()
    {
        return getRecord( 1 );
    }

    public void setRandomNumber( long nr )
    {
        setRecord( 1, nr );
    }

    public void setRecoveredStatus( boolean status )
    {
        if ( status )
        {
            setRecovered();
        }
        else
        {
            unsetRecovered();
        }
    }

    public long getVersion()
    {
        return getRecord( 2 );
    }

    public void setVersion( long version )
    {
        setRecord( 2, version );
    }

    public synchronized void setLastCommittedTx( long txId )
    {
        long current = getRecord( 3 );
        if ( (current + 1) != txId && !isInRecoveryMode() )
        {
            throw new InvalidRecordException( "Could not set tx commit id[" +
                txId + "] since the current one is[" + current + "]" );
        }
        setRecord( 3, txId );
        // TODO Why check null here? because I have no time to fix the tests
        // And the update to zookeeper or whatever should probably be moved from
        // here and be async since if it fails tx will get exception in committing
        // state and shutdown... that is wrong since the tx did not fail
        // - zookeeper is only used for master election, tx state there is not critical
        if ( isStarted && lastCommittedTxIdSetter != null && txId != lastCommittedTx )
        {
            try
            {
                lastCommittedTxIdSetter.setLastCommittedTxId( txId );
            }
            catch ( RuntimeException e )
            {
                logger.log( Level.WARNING, "Could not set last committed tx id", e );
            }
        }
        lastCommittedTx = txId;
    }

    public synchronized long getLastCommittedTx()
    {
        if ( lastCommittedTx == -1 )
        {
            lastCommittedTx = getRecord( 3 );
        }
        return lastCommittedTx;
    }

    public long incrementVersion()
    {
        long current = getVersion();
        setVersion( current + 1 );
        return current;
    }

    private long getRecord( long id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            buffer.get();
            return buffer.getLong();
        }
        finally
        {
            releaseWindow( window );
        }
    }

    private void setRecord( long id, long value )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.WRITE );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            buffer.put( Record.IN_USE.byteValue() ).putLong( value );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    public long getStoreVersion()
    {
        return getRecord( 4 );
    }

    public void setStoreVersion( long version )
    {
        setRecord( 4, version );
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
    public RelationshipTypeStore getRelationshipTypeStore()
    {
        return relTypeStore;
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

    @Override
    public void makeStoreOk()
    {
        relTypeStore.makeStoreOk();
        propStore.makeStoreOk();
        relStore.makeStoreOk();
        nodeStore.makeStoreOk();
        super.makeStoreOk();
        isStarted = true;
    }

    @Override
    public void rebuildIdGenerators()
    {
        relTypeStore.rebuildIdGenerators();
        propStore.rebuildIdGenerators();
        relStore.rebuildIdGenerators();
        nodeStore.rebuildIdGenerators();
        super.rebuildIdGenerators();
    }

    public void updateIdGenerators()
    {
        this.updateHighId();
        relTypeStore.updateIdGenerators();
        propStore.updateIdGenerators();
        relStore.updateHighId();
        nodeStore.updateHighId();
    }

    public int getRelationshipGrabSize()
    {
        return REL_GRAB_SIZE;
    }

    @Override
    public List<WindowPoolStats> getAllWindowPoolStats()
    {
        List<WindowPoolStats> list = new ArrayList<WindowPoolStats>();
        list.addAll( nodeStore.getAllWindowPoolStats() );
        list.addAll( propStore.getAllWindowPoolStats() );
        list.addAll( relStore.getAllWindowPoolStats() );
        list.addAll( relTypeStore.getAllWindowPoolStats() );
        return list;
    }

    public boolean isStoreOk()
    {
        return getStoreOk() && relTypeStore.getStoreOk() &&
            propStore.getStoreOk() && relStore.getStoreOk() && nodeStore.getStoreOk();
    }

    @Override
    public void logVersions( StringLogger msgLog )
    {
        super.logVersions( msgLog );
        nodeStore.logVersions( msgLog );
        relStore.logVersions( msgLog );
        relTypeStore.logVersions( msgLog );
        propStore.logVersions( msgLog );
    }

    public void logIdUsage( StringLogger msgLog )
    {
        nodeStore.logIdUsage( msgLog );
        relStore.logIdUsage( msgLog );
        relTypeStore.logIdUsage( msgLog );
        propStore.logIdUsage( msgLog );
    }

    public static void logIdUsage( StringLogger logger, Store store )
    {
        logger.logMessage( String.format( "  %s: used=%s high=%s", store.getTypeDescriptor(),
                store.getNumberOfIdsInUse(), store.getHighestPossibleIdInUse() ) );
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
            throw new IllegalArgumentException(
                    String.format(
                            "The given string %s is not of proper size for a store version string",
                            storeVersion ) );
        }
        bits.put( length, 8 );
        for ( int i = 0; i < length; i++ )
        {
            char c = storeVersion.charAt( i );
            if ( c < 0 || c >= 256 )
                throw new IllegalArgumentException(
                        String.format(
                                "Store version strings should be encode-able as Latin1 - %s is not",
                                storeVersion ) );
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
            throw new IllegalArgumentException( String.format(
                    "The read in version string length %d is not proper.",
                    length ) );
        }
        char[] result = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (char) bits.getShort( 8 );
        }
        return new String( result );
    }

    public static void main( String[] args )
    {
        long result = versionStringToLong( "f123oo" );
        String back = versionLongToString( result );
        System.out.println( result );
        System.out.println( back );
    }
}
