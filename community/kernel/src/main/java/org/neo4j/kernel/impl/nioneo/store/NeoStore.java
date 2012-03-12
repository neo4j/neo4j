/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.List;
import java.util.logging.Level;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
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
    public interface Configuration
        extends AbstractStore.Configuration
    {
        int relationship_grab_size(int defaultRelGrabSize);
    }

    public static final String TYPE_DESCRIPTOR = "NeoStore";

    /*
     *  6 longs in header (long + in use), time | random | version | txid | store version | graph next prop
     */
    public static final int RECORD_SIZE = 9;
    private static final int DEFAULT_REL_GRAB_SIZE = 100;

    public static final String DEFAULT_NAME = "neostore";

    private NodeStore nodeStore;
    private PropertyStore propStore;
    private RelationshipStore relStore;
    private RelationshipTypeStore relTypeStore;
    private final TxHook txHook;
    private boolean isStarted;
    private long lastCommittedTx = -1;

    private final int REL_GRAB_SIZE;
    private final String fileName;
    private final Configuration conf;
    private final LastCommittedTxIdSetter lastCommittedTxIdSetter;

    public NeoStore(String fileName, Configuration conf,
                    LastCommittedTxIdSetter lastCommittedTxIdSetter,
                    IdGeneratorFactory idGeneratorFactory, FileSystemAbstraction fileSystemAbstraction,
                    StringLogger stringLogger, TxHook txHook,
                    RelationshipTypeStore relTypeStore, PropertyStore propStore, RelationshipStore relStore, NodeStore nodeStore)
    {
        super( fileName, conf, IdType.NEOSTORE_BLOCK, idGeneratorFactory, fileSystemAbstraction, stringLogger);
        this.fileName = fileName;
        this.conf = conf;
        this.lastCommittedTxIdSetter = lastCommittedTxIdSetter;
        this.relTypeStore = relTypeStore;
        this.propStore = propStore;
        this.relStore = relStore;
        this.nodeStore = nodeStore;
        REL_GRAB_SIZE = conf.relationship_grab_size(DEFAULT_REL_GRAB_SIZE);
        this.txHook = txHook;

        /* [MP:2012-01-03] Fix for the problem in 1.5.M02 where store version got upgraded but
         * corresponding store version record was not added. That record was added in the release
         * thereafter so this missing record doesn't trigger an upgrade of the neostore file and so any
         * unclean shutdown on such a db with 1.5.M02 < neo4j version <= 1.6.M02 would make that
         * db unable to start for that version with a "Mismatching store version found" exception.
         *
         * This will make a cleanly shut down 1.5.M02, then started and cleanly shut down with 1.6.M03 (or higher)
         * successfully add the missing record.
         */
        setRecovered();
        try
        {
            if ( getCreationTime() != 0 /*Store that wasn't just now created*/ &&
                    getStoreVersion() == 0 /*Store is missing the store version record*/ )
            {
                setStoreVersion( versionStringToLong( CommonAbstractStore.ALL_STORES_VERSION ) );
                updateHighId();
            }
        }
        finally
        {
            unsetRecovered();
        }
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
                String foundVersion = versionLongToString( getStoreVersion(fileSystemAbstraction, configuration.neo_store()) );
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
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to check version "
                    + getStorageFileName(), e );
        }
    }

    @Override
    protected void verifyFileSizeAndTruncate() throws IOException
    {
        super.verifyFileSizeAndTruncate();

        /* MP: 2011-11-23
         * A little silent upgrade for the "next prop" record. It adds one record last to the neostore file.
         * It's backwards compatible, that's why it can be a silent and automatic upgrade.
         */
        if ( getFileChannel().size() == RECORD_SIZE*5 )
        {
            insertRecord( 5, -1 );
            registerIdFromUpdateRecord( 5 );
        }
    }

    private void insertRecord( int recordPosition, long value ) throws IOException
    {
        try
        {
            FileChannel channel = getFileChannel();
            long previousPosition = channel.position();
            channel.position( RECORD_SIZE*recordPosition );
            int trail = (int) (channel.size()-channel.position());
            ByteBuffer trailBuffer = null;
            if ( trail > 0 )
            {
                trailBuffer = ByteBuffer.allocate( trail );
                channel.read( trailBuffer );
                trailBuffer.flip();
            }
            ByteBuffer buffer = ByteBuffer.allocate( RECORD_SIZE );
            buffer.put( Record.IN_USE.byteValue() );
            buffer.putLong( value );
            buffer.flip();
            channel.position( RECORD_SIZE*recordPosition );
            channel.write( buffer );
            if ( trail > 0 )
            {
                channel.write( trailBuffer );
            }
            channel.position( previousPosition );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    public boolean freeIdsDuringRollback()
    {
        return txHook.freeIdsDuringRollback();
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

    public static long getStoreVersion( FileSystemAbstraction fs, String storeDir )
    {
        return getRecord( fs, storeDir, 4 );
    }

    public static long getTxId( FileSystemAbstraction fs, String storeDir )
    {
        return getRecord( fs, storeDir, 3 );
    }

    private static long getRecord( FileSystemAbstraction fs, String storeDir, long recordPosition )
    {
        FileChannel channel = null;
        try
        {
            channel = fs.open( storeDir, "rw" );
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
                if ( channel != null ) channel.close();
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
                lastCommittedTxIdSetter.setLastCommittedTxId(txId);
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
            registerIdFromUpdateRecord( id );
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

    public long getGraphNextProp()
    {
        return getRecord( 5 );
    }

    public void setGraphNextProp( long propId )
    {
        setRecord( 5, propId );
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
    public void logVersions( StringLogger.LineLogger msgLog)
    {
        msgLog.logLine( "Store versions:" );

        super.logVersions( msgLog );
        nodeStore.logVersions( msgLog );
        relStore.logVersions( msgLog );
        relTypeStore.logVersions( msgLog );
        propStore.logVersions(msgLog  );

        stringLogger.flush();
    }

    @Override
    public void logIdUsage( StringLogger.LineLogger msgLog )
    {
        msgLog.logLine( "Id usage:" );
        nodeStore.logIdUsage(msgLog );
        relStore.logIdUsage(msgLog );
        relTypeStore.logIdUsage( msgLog);
        propStore.logIdUsage( msgLog );
        stringLogger.flush();
    }

    public NeoStoreRecord asRecord()
    {
        NeoStoreRecord result = new NeoStoreRecord();
        result.setNextProp( getRecord( 5 ) );
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
        Bits bits = Bits.bits(8);
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
        Bits bits = Bits.bitsFromLongs(new long[]{storeVersion});
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
}
