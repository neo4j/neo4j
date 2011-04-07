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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.logging.Logger;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;

/**
 * Contains common implementation for {@link AbstractStore} and
 * {@link AbstractDynamicStore}.
 */
public abstract class CommonAbstractStore
{
    protected static final Logger logger = Logger
        .getLogger( CommonAbstractStore.class.getName() );
    
    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base|modifier;
    }

    private long highestUpdateRecordId = -1;

    /**
     * Returns the type and version that identifies this store.
     *
     * @return This store's implementation type and version identifier
     */
    public abstract String getTypeAndVersionDescriptor();

    /**
     * Called from the constructor after the end header has been checked. The
     * store implementation can setup it's
     * {@link PersistenceWindow persistence windows} and other resources that
     * are needed by overriding this implementation.
     * <p>
     * This default implementation does nothing.
     *
     * @throws IOException
     *             If unable to initialize
     */
    protected void initStorage()
    {
    }

    protected boolean versionFound( String version )
    {
        return true;
    }

    /**
     * This method should close/release all resources that the implementation of
     * this store has allocated and is called just before the <CODE>close()</CODE>
     * method returns. Override this method to clean up stuff created in
     * {@link #initStorage()} method.
     * <p>
     * This default implementation does nothing.
     *
     * @throws IOException
     *             If unable to close
     */
    protected void closeStorage()
    {
    };

    /**
     * Should do first validation on store validating stuff like version and id
     * generator. This method is called by constructors.
     *
     * @throws IOException
     *             If unable to load store
     */
    protected abstract void loadStorage();

    /**
     * Should rebuild the id generator from scratch.
     *
     * @throws IOException
     *             If unable to rebuild id generator.
     */
    protected abstract void rebuildIdGenerator();

    // default node store id generator grab size
    protected static final int DEFAULT_ID_GRAB_SIZE = 1024;

    private final String storageFileName;
    private final IdGeneratorFactory idGeneratorFactory;
    private IdGenerator idGenerator = null;
    private FileChannel fileChannel = null;
    private PersistenceWindowPool windowPool;
    private boolean storeOk = true;
    private FileLock fileLock;
    private boolean grabFileLock = true;

    private Map<?,?> config = null;

    private boolean readOnly = false;
    private boolean backupSlave = false;

    private final IdType idType;

    /**
     * Opens and validates the store contained in <CODE>fileName</CODE>
     * loading any configuration defined in <CODE>config</CODE>. After
     * validation the <CODE>initStorage</CODE> method is called.
     * <p>
     * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>
     * and the {@link #validate()} method will not throw exception when invoked.
     * If a problem was found when opening the store the {@link #makeStoreOk()}
     * must be invoked else {@link #validate()} will throw exception.
     *
     * throws IOException if the unable to open the storage or if the
     * <CODE>initStorage</CODE> method fails
     *
     * @param fileName
     *            The name of the store
     * @param config
     *            The configuration for store (may be null)
     * @throws IOException
     *             If store doesn't exist
     */
    public CommonAbstractStore( String fileName, Map<?,?> config, IdType idType )
    {
        this.storageFileName = fileName;
        this.config = config;
        this.idType = idType;
        if ( config != null )
        {
            String fileLock = (String) config.get( "grab_file_lock" );
            if ( fileLock != null && fileLock.toLowerCase().equals( "false" ) )
            {
                grabFileLock = false;
            }
        }
        this.idGeneratorFactory = (IdGeneratorFactory)
                config.get( IdGeneratorFactory.class );
        
//        try
//        {
            checkStorage();
            loadStorage();
            initStorage();
//        }
//        catch ( RuntimeException e )
//        {
//            closeFileChannelIfOpened();
//            if ( idGenerator != null )
//            {
//                idGenerator.close();
//                idGenerator = null;
//            }
//            closeStorage();
//            throw e;
//        }
    }

    boolean isReadOnly()
    {
        return readOnly;
    }

    boolean isBackupSlave()
    {
        return backupSlave;
    }

    /**
     * Opens and validates the store contained in <CODE>fileName</CODE>.
     * After validation the <CODE>initStorage</CODE> method is called.
     * <p>
     * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>
     * and the {@link #validate()} method will not throw exception when invoked.
     * If a problem was found when opening the store the {@link #makeStoreOk()}
     * must be invoked else {@link #validate()} will throw exception.
     *
     * throws IOException if the unable to open the storage or if the
     * <CODE>initStorage</CODE> method fails
     *
     * @param fileName
     *            The name of the store
     * @throws IOException
     *             If store doesn't exist
     */
//    public CommonAbstractStore( String fileName )
//    {
//        this.storageFileName = fileName;
//        idGeneratorFactory = IdGeneratorFactory.DEFAULT;
//        checkStorage();
//        loadStorage();
//        initStorage();
//    }
    
    protected FileSystemAbstraction getFileSystem()
    {
        return (FileSystemAbstraction) config.get( FileSystemAbstraction.class );
    }

    private void checkStorage()
    {
        if ( config != null )
        {
            Boolean isReadOnly = Boolean.parseBoolean( (String) config.get( Config.READ_ONLY ) );
            if ( isReadOnly != null )
            {
                readOnly = isReadOnly;
            }
        }
        if ( config != null )
        {
            String str = (String) config.get( "backup_slave" );
            if ( "true".equals( str ) )
            {
                backupSlave = true;
            }
        }
        if ( !new File( storageFileName ).exists() )
        {
            throw new IllegalStateException( "No such store[" + storageFileName
                + "]" );
        }
        try
        {
            this.fileChannel = getFileSystem().open( storageFileName, readOnly ? "r" : "rw" );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open file "
                + storageFileName, e );
        }
        try
        {
            if ( (!readOnly || backupSlave) && grabFileLock )
            {
                this.fileLock = getFileSystem().tryLock( storageFileName, fileChannel );
                if ( fileLock == null )
                {
                    throw new IllegalStateException( "Unable to lock store ["
                        + storageFileName + "], this is usually a result of some "
                        + "other Neo4j kernel running using the same store." );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to lock store["
                + storageFileName + "]" );
        }
        catch ( OverlappingFileLockException e )
        {
            throw new IllegalStateException( "Unable to lock store [" + storageFileName +
                    "], this is usually caused by another Neo4j kernel already running in " +
                    "this JVM for this particular store" );
        }
    }

    private void closeFileChannelIfOpened()
    {
        if ( this.fileChannel != null )
        {
            try
            {
                this.fileChannel.close();
                this.fileChannel = null;
            }
            catch ( IOException e )
            {
                // Unable to close!
            }
        }
    }

    /**
     * Marks this store as "not ok".
     *
     */
    protected void setStoreNotOk()
    {
        if ( readOnly && !isBackupSlave() )
        {
            throw new UnderlyingStorageException(
                "Cannot start up on non clean store as read only" );
        }
        storeOk = false;
    }

    /**
     * If store is "not ok" <CODE>false</CODE> is returned.
     *
     * @return True if this store is ok
     */
    protected boolean getStoreOk()
    {
        return storeOk;
    }

    /**
     * Sets the {@link PersistenceWindowPool} for this store to use. Normally
     * this is set in the {@link #loadStorage()} method. This method must be
     * invoked with a valid "pool" before any of the
     * {@link #acquireWindow(long, OperationType)}
     * {@link #releaseWindow(PersistenceWindow)} {@link #flush(int)}
     * {@link #forget(int)} {@link #close()} methods are invoked.
     *
     * @param pool
     *            The window pool this store should use
     */
    protected void setWindowPool( PersistenceWindowPool pool )
    {
        this.windowPool = pool;
    }

    /**
     * Returns the next id for this store's {@link IdGenerator}.
     *
     * @return The next free id
     * @throws IOException
     *             If unable to get next free id
     */
    public long nextId()
    {
        return idGenerator.nextId();
    }

    /**
     * Frees an id for this store's {@link IdGenerator}.
     *
     * @param id
     *            The id to free
     * @throws IOException
     *             If unable to free the id
     */
    public void freeId( long id )
    {
        idGenerator.freeId( id ); // makeUnsignedInt( id ) );
    }

//    private long makeUnsignedInt( int signedInteger )
//    {
//        return signedInteger & 0xFFFFFFFFL;
//    }

    /**
     * Return the highest id in use.
     *
     * @return The highest id in use.
     */
    public long getHighId()
    {
        long genHighId = idGenerator != null ? idGenerator.getHighId() : -1;
        long updateHighId = highestUpdateRecordId;
        if ( updateHighId > genHighId )
        {
            return updateHighId;
        }
        return genHighId;
    }

    /**
     * Sets the highest id in use (use this when rebuilding id generator).
     *
     * @param highId
     *            The high id to set.
     */
    public void setHighId( long highId )
    {
        if ( idGenerator != null )
        {
            idGenerator.setHighId( highId );
        }
    }

    protected boolean getIfMemoryMapped()
    {
        String configValue = getConfig() != null ?
                (String) getConfig().get( Config.USE_MEMORY_MAPPED_BUFFERS ) : null;
        return configValue != null ? Boolean.parseBoolean( configValue ) : true;
    }

    /**
     * Returns memory assigned for
     * {@link MappedPersistenceWindow memory mapped windows} in bytes. The
     * configuration map passed in one constructor is checked for an entry with
     * this stores name.
     *
     * @return The number of bytes memory mapped windows this store has
     */
    protected long getMappedMem()
    {
        if ( getConfig() != null )
        {
            String convertSlash = storageFileName.replace( '\\', '/' );
            String realName = convertSlash.substring( convertSlash
                .lastIndexOf( '/' ) + 1 );
            String mem = (String) getConfig().get( realName + ".mapped_memory" );
            if ( mem != null )
            {
                long multiplier = 1;
                if ( mem.endsWith( "M" ) )
                {
                    multiplier = 1024 * 1024;
                    mem = mem.substring( 0, mem.length() - 1 );
                }
                else if ( mem.endsWith( "k" ) )
                {
                    multiplier = 1024;
                    mem = mem.substring( 0, mem.length() - 1 );
                }
                else if ( mem.endsWith( "G" ) )
                {
                    multiplier = 1024*1024*1024;
                    mem = mem.substring( 0, mem.length() - 1 );
                }
                try
                {
                    return Integer.parseInt( mem ) * multiplier;
                }
                catch ( NumberFormatException e )
                {
                    logger.info( "Unable to parse mapped memory[" + mem
                        + "] string for " + storageFileName );
                }
            }
        }
        return 0;
    }

    /**
     * If store is not ok a call to this method will rebuild the {@link
     * IdGenerator} used by this store and if successful mark it as
     * <CODE>ok</CODE>.
     *
     * @throws IOException
     *             If unable to rebuild id generator
     */
    public void makeStoreOk()
    {
        if ( !storeOk )
        {
            if ( readOnly && !backupSlave )
            {
                throw new ReadOnlyDbException();
            }
            rebuildIdGenerator();
            storeOk = true;
        }
    }

    public void rebuildIdGenerators()
    {
        if ( readOnly && !backupSlave )
        {
            throw new ReadOnlyDbException();
        }
        rebuildIdGenerator();
    }

    /**
     * Returns the configuration map if set in constructor.
     *
     * @return A map containing configuration or <CODE>null<CODE> if no
     *         configuration map set.
     */
    public Map<?,?> getConfig()
    {
        return config;
    }
    
    /**
     * @return the store directory from config.
     */
    protected String getStoreDir()
    {
        return (String) config.get( "store_dir" );
    }

    /**
     * Acquires a {@link PersistenceWindow} for <CODE>position</CODE> and
     * operation <CODE>type</CODE>. Window must be released after operation
     * has been performed via {@link #releaseWindow(PersistenceWindow)}.
     *
     * @param position
     *            The record position
     * @param type
     *            The operation type
     * @return a persistence window encapsulating the record
     * @throws IOException
     *             If unable to acquire window
     */
    protected PersistenceWindow acquireWindow( long position, OperationType type )
    {
        if ( !isInRecoveryMode() && ( position > getHighId() || !storeOk) )
        {
            throw new InvalidRecordException( "Position[" + position
                + "] requested for operation is high id["
                + getHighId() + "], store is ok[" + storeOk + "]" );
        }
        return windowPool.acquire( position, type );
    }

    /**
     * Releases the window and writes the data (async) if the
     * <CODE>window</CODE> was a {@link PersistenceRow}.
     *
     * @param window
     *            The window to be released
     * @throws IOException
     *             If window was a <CODE>DirectPersistenceRow</CODE> and
     *             unable to write out its data to the store
     */
    protected void releaseWindow( PersistenceWindow window )
    {
        windowPool.release( window );
    }

    public void flushAll()
    {
        windowPool.flushAll();
    }

    private boolean isRecovered = false;

    protected boolean isInRecoveryMode()
    {
        return isRecovered;
    }

    protected void setRecovered()
    {
        isRecovered = true;
    }

    protected void unsetRecovered()
    {
        isRecovered = false;
    }

    /**
     * Returns the name of this store.
     *
     * @return The name of this store
     */
    public String getStorageFileName()
    {
        return storageFileName;
    }

    /**
     * Opens the {@link IdGenerator} used by this store.
     *
     * @throws IOException
     *             If unable to open the id generator
     */
    protected void openIdGenerator()
    {
        idGenerator = openIdGenerator( storageFileName + ".id",
            DEFAULT_ID_GRAB_SIZE );
    }
    
    protected IdGenerator openIdGenerator( String fileName, int grabSize )
    {
        return idGeneratorFactory.open( fileName, grabSize, getIdType(),
                figureOutHighestIdInUse() );
    }
    
    protected abstract long figureOutHighestIdInUse();

    protected void createIdGenerator( String fileName )
    {
        idGeneratorFactory.create( fileName );
    }
    
    protected void openReadOnlyIdGenerator( int recordSize )
    {
        try
        {
            idGenerator = new ReadOnlyIdGenerator( storageFileName + ".id",
                fileChannel.size() / recordSize );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Closed the {@link IdGenerator} used by this store
     *
     * @throws IOException
     *             If unable to close this store
     */
    protected void closeIdGenerator()
    {
        if ( idGenerator != null )
        {
            idGenerator.close();
        }
    }

    /**
     * Closes this store. This will cause all buffers and channels to be closed.
     * Requesting an operation from after this method has been invoked is
     * illegal and an exception will be thrown.
     * <p>
     * This method will start by invoking the {@link #closeStorage} method
     * giving the implementing store way to do anything that it needs to do
     * before the fileChannel is closed.
     *
     * @throws IOException
     *             If problem when invoking {@link #closeStorage()}
     */
    public void close()
    {
        if ( fileChannel == null )
        {
            return;
        }
        closeStorage();
        if ( windowPool != null )
        {
            windowPool.close();
            windowPool = null;
        }
        if ( isReadOnly() && !isBackupSlave() )
        {
            try
            {
                fileChannel.close();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
            return;
        }
        long highId = idGenerator.getHighId();
        int recordSize = -1;
        if ( this instanceof AbstractDynamicStore )
        {
            recordSize = ((AbstractDynamicStore) this).getBlockSize();
        }
        else if ( this instanceof AbstractStore )
        {
            recordSize = ((AbstractStore) this).getRecordSize();
        }
        closeIdGenerator();
        boolean success = false;
        IOException storedIoe = null;
        // hack for WINBLOWS
        if ( !readOnly || backupSlave )
        {
            for ( int i = 0; i < 10; i++ )
            {
                try
                {
                    fileChannel.position( highId * recordSize );
                    ByteBuffer buffer = ByteBuffer.wrap(
                        UTF8.encode( getTypeAndVersionDescriptor() ) );
                    fileChannel.write( buffer );
                    fileChannel.truncate( fileChannel.position() );
                    fileChannel.force( false );
                    if ( fileLock != null )
                    {
                        fileLock.release();
                    }
                    fileChannel.close();
                    fileChannel = null;
                    success = true;
                    break;
                }
                catch ( IOException e )
                {
                    storedIoe = e;
                    System.gc();
                }
            }
        }
        else
        {
            try
            {
                fileChannel.close();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        if ( !success )
        {
            throw new UnderlyingStorageException( "Unable to close store "
                + getStorageFileName(), storedIoe );
        }
    }

    /**
     * Returns a <CODE>FileChannel</CODE> to this storage's file. If
     * <CODE>close()</CODE> method has been invoked <CODE>null</CODE> will be
     * returned.
     *
     * @return A file channel to this storage
     */
    protected final FileChannel getFileChannel()
    {
        return fileChannel;
    }

    /**
     * @return The highest possible id in use, -1 if no id in use.
     */
    public long getHighestPossibleIdInUse()
    {
        return idGenerator.getHighId() - 1;
    }

    /**
     * @return The total number of ids in use.
     */
    public long getNumberOfIdsInUse()
    {
        return idGenerator.getNumberOfIdsInUse();
    }

    public WindowPoolStats getWindowPoolStats()
    {
        return windowPool.getStats();
    }
    
    public IdType getIdType()
    {
        return idType;
    }

    protected void registerIdFromUpdateRecord( long id )
    {
        highestUpdateRecordId = Math.max( highestUpdateRecordId, id + 1 );
    }

    protected void updateHighId()
    {
        long highId = highestUpdateRecordId;
        highestUpdateRecordId = -1;
        if ( highId > getHighId() )
        {
            setHighId( highId );
        }
    }
}