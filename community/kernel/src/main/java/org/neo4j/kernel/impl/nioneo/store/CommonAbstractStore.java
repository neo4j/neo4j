/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.nio.channels.OverlappingFileLockException;
import java.util.LinkedList;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.FileUtils.FileOperation;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.nio.ByteBuffer.wrap;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.kernel.impl.util.FileUtils.windowsSafeIOOperation;

/**
 * Contains common implementation for {@link AbstractStore} and
 * {@link AbstractDynamicStore}.
 */
public abstract class CommonAbstractStore implements IdSequence, AutoCloseable
{
    public static abstract class Configuration
    {
        public static final Setting<File> store_dir = InternalAbstractGraphDatabase.Configuration.store_dir;
        public static final Setting<File> neo_store = InternalAbstractGraphDatabase.Configuration.neo_store;

        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
        public static final Setting<Boolean> backup_slave = GraphDatabaseSettings.backup_slave;
        public static final Setting<Boolean> use_memory_mapped_buffers = GraphDatabaseSettings.use_memory_mapped_buffers;

        public static final Setting<Boolean> rebuild_idgenerators_fast = GraphDatabaseSettings.rebuild_idgenerators_fast;
    }

    public static final String ALL_STORES_VERSION = "v0.A.3";
    public static final String UNKNOWN_VERSION = "Unknown";

    protected Config configuration;
    private final IdGeneratorFactory idGeneratorFactory;
    private final WindowPoolFactory windowPoolFactory;
    protected FileSystemAbstraction fileSystemAbstraction;

    protected final File storageFileName;
    protected final IdType idType;
    protected StringLogger stringLogger;
    private IdGenerator idGenerator = null;
    private StoreChannel fileChannel = null;
    private WindowPool windowPool;
    private boolean storeOk = true;
    private Throwable causeOfStoreNotOk;
    private FileLock fileLock;

    private boolean readOnly = false;
    private boolean backupSlave = false;
    private long highestUpdateRecordId = -1;
    private final StoreVersionMismatchHandler versionMismatchHandler;

    /**
     * Opens and validates the store contained in <CODE>fileName</CODE>
     * loading any configuration defined in <CODE>config</CODE>. After
     * validation the <CODE>initStorage</CODE> method is called.
     * <p>
     * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>
     * and the {@link #getStoreOk()} method will return true.
     * If a problem was found when opening the store the {@link #makeStoreOk()}
     * must be invoked.
     *
     * throws IOException if the unable to open the storage or if the
     * <CODE>initStorage</CODE> method fails
     *
     * @param idType The Id used to index into this store
     */
    public CommonAbstractStore( File fileName, Config configuration, IdType idType,
                                IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                                FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger,
                                StoreVersionMismatchHandler versionMismatchHandler )
    {
        this.storageFileName = fileName;
        this.configuration = configuration;
        this.idGeneratorFactory = idGeneratorFactory;
        this.windowPoolFactory = windowPoolFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.idType = idType;
        this.stringLogger = stringLogger;
        this.versionMismatchHandler = versionMismatchHandler;

        try
        {
            checkStorage();
            checkVersion(); // Overriden in NeoStore
            loadStorage();
        }
        catch ( Exception e )
        {
            releaseFileLockAndCloseFileChannel();
            throw launderedException( e );
        }
    }

    public String getTypeAndVersionDescriptor()
    {
        return buildTypeDescriptorAndVersion( getTypeDescriptor() );
    }

    public static String buildTypeDescriptorAndVersion( String typeDescriptor )
    {
        return buildTypeDescriptorAndVersion( typeDescriptor, ALL_STORES_VERSION );
    }

    public static String buildTypeDescriptorAndVersion( String typeDescriptor, String version )
    {
        return typeDescriptor + " " + version;
    }

    protected long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    /**
     * Returns the type and version that identifies this store.
     *
     * @return This store's implementation type and version identifier
     */
    public abstract String getTypeDescriptor();

    protected void checkStorage()
    {
        readOnly = configuration.get( Configuration.read_only );
        backupSlave = configuration.get( Configuration.backup_slave );
        if ( !fileSystemAbstraction.fileExists( storageFileName ) )
        {
            throw new StoreNotFoundException( "No such store[" + storageFileName + "] in " + fileSystemAbstraction );
        }
        try
        {
            this.fileChannel = fileSystemAbstraction.open( storageFileName, readOnly ? "r" : "rw" );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open file " + storageFileName, e );
        }
        try
        {
            if ( !readOnly || backupSlave )
            {
                this.fileLock = fileSystemAbstraction.tryLock( storageFileName, fileChannel );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to lock store[" + storageFileName + "]", e );
        }
        catch ( OverlappingFileLockException e )
        {
            throw new IllegalStateException( "Unable to lock store [" + storageFileName +
                                             "], this is usually caused by another Neo4j kernel already running in " +
                                             "this JVM for this particular store" );
        }
    }

    protected void checkVersion()
    {
        try
        {
            verifyCorrectTypeDescriptorAndVersion();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to check version " + getStorageFileName(), e );
        }
    }

    /**
     * Should do first validation on store validating stuff like version and id
     * generator. This method is called by constructors.
     */
    protected void loadStorage()
    {
        try
        {
            readAndVerifyBlockSize();
            verifyFileSizeAndTruncate();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to load storage " + getStorageFileName(), e );
        }
        loadIdGenerator();

        this.windowPool = windowPoolFactory.create( getStorageFileName(), getEffectiveRecordSize(),
                getFileChannel(), configuration, stringLogger, getNumberOfReservedLowIds() );
    }

    protected abstract int getEffectiveRecordSize();

    protected abstract void verifyFileSizeAndTruncate() throws IOException;

    protected abstract void readAndVerifyBlockSize() throws IOException;

    private void loadIdGenerator()
    {
        try
        {
            if ( !isReadOnly() || isBackupSlave() )
            {
                openIdGenerator();
            }
            else
            {
                openReadOnlyIdGenerator( getEffectiveRecordSize() );
            }
        }
        catch ( InvalidIdGeneratorException e )
        {
            setStoreNotOk( e );
        }
        finally
        {
            if ( !getStoreOk() )
            {
                if ( stringLogger != null )
                {
                    stringLogger.debug( getStorageFileName() + " non clean shutdown detected" );
                }
            }
        }
    }

    protected void verifyCorrectTypeDescriptorAndVersion() throws IOException
    {
        String expectedTypeDescriptorAndVersion = getTypeAndVersionDescriptor();
        int length = UTF8.encode( expectedTypeDescriptorAndVersion ).length;
        byte bytes[] = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap( bytes );
        long fileSize = getFileChannel().size();
        if ( fileSize >= length )
        {
            getFileChannel().position( fileSize - length );
        }
        else if ( !isReadOnly() )
        {
            setStoreNotOk( new IllegalStateException(
                    "Invalid file size " + fileSize + " for " + this + ". Expected " + length + " or bigger" ) );
            return;
        }
        getFileChannel().read( buffer );
        String foundTypeDescriptorAndVersion = UTF8.decode( bytes );

        if ( !expectedTypeDescriptorAndVersion.equals( foundTypeDescriptorAndVersion ) && !isReadOnly() )
        {
            if ( foundTypeDescriptorAndVersion.startsWith( getTypeDescriptor() ) )
            {
                versionMismatchHandler.mismatch( ALL_STORES_VERSION, foundTypeDescriptorAndVersion );
            }
            else
            {
                setStoreNotOk( new IllegalStateException(
                        "Unexpected version " + foundTypeDescriptorAndVersion + ", expected " +
                        expectedTypeDescriptorAndVersion ) );
            }
        }
    }

    /** Should rebuild the id generator from scratch. */
    protected void rebuildIdGenerator()
    {
        if ( isReadOnly() && !isBackupSlave() )
        {
            throw new ReadOnlyDbException();
        }

        stringLogger.debug( "Rebuilding id generator for[" + getStorageFileName() + "] ..." );
        closeIdGenerator();
        File idFile = new File( getStorageFileName().getPath() + ".id" );
        if ( fileSystemAbstraction.fileExists( idFile ) )
        {
            boolean success = fileSystemAbstraction.deleteFile( idFile );
            assert success : "Couldn't delete " + idFile.getPath() + ", still open?";
        }
        createIdGenerator( idFile );
        openIdGenerator();
        setHighId( getNumberOfReservedLowIds() );
        StoreChannel fileChannel = getFileChannel();
        boolean fastRebuild = doFastIdGeneratorRebuild();
        long defraggedCount = 0;
        try
        {
            long fileSize = fileChannel.size();
            int recordSize = getRecordSize();
            if ( fastRebuild )
            {
                setHighId( findHighIdBackwards() );
            }
            else
            {
                defraggedCount = fullScanIdRebuild( fileChannel, fileSize, recordSize );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to rebuild id generator " + getStorageFileName(), e );
        }
        stringLogger.debug( getStorageFileName() + " rebuild id generator, highId=" + getHighId() +
                            " defragged count=" + defraggedCount );
        stringLogger.debug( "[" + getStorageFileName() + "] high id=" + getHighId() +
                " (defragged=" + defraggedCount + ")" );

        if ( !fastRebuild )
        {
            closeIdGenerator();
            openIdGenerator();
        }
    }

    private long fullScanIdRebuild( StoreChannel fileChannel, long fileSize, int recordSize ) throws IOException
    {
        long defraggedCount = 0;
        ByteBuffer byteBuffer = ByteBuffer.allocate( recordSize );
        LinkedList<Long> freeIdList = new LinkedList<>();
        for ( long i = getNumberOfReservedLowIds(); i * recordSize < fileSize; i++ )
        {
            fileChannel.position( i * recordSize );
            byteBuffer.clear();
            fileChannel.read( byteBuffer );
            byteBuffer.flip();
            if ( !isRecordInUse( byteBuffer ) )
            {
                freeIdList.add( i );
            }
            else if ( isRecordReserved( byteBuffer ) )
            {
                byteBuffer.clear();
                byteBuffer.put( Record.NOT_IN_USE.byteValue() ).putInt( 0 );
                byteBuffer.flip();
                fileChannel.write( byteBuffer, i * recordSize );
                freeIdList.add( i );
            }
            else
            {
                setHighId( i + 1 );
                while ( !freeIdList.isEmpty() )
                {
                    freeId( freeIdList.removeFirst() );
                    defraggedCount++;
                }
            }
        }
        return defraggedCount;
    }

    protected boolean doFastIdGeneratorRebuild()
    {
        return configuration.get( Configuration.rebuild_idgenerators_fast );
    }

    /**
     * This method should close/release all resources that the implementation of
     * this store has allocated and is called just before the <CODE>close()</CODE>
     * method returns. Override this method to clean up stuff the constructor.
     * <p>
     * This default implementation does nothing.
     */
    protected void closeStorage()
    {
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
     * Marks this store as "not ok".
     */
    protected void setStoreNotOk( Throwable cause )
    {
        if ( readOnly && !isBackupSlave() )
        {
            throw new UnderlyingStorageException(
                    "Cannot start up on non clean store as read only" );
        }
        storeOk = false;
        causeOfStoreNotOk = cause;
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
     * Returns the next id for this store's {@link IdGenerator}.
     *
     * @return The next free id
     */
    @Override public long nextId()
    {
        return idGenerator.nextId();
    }

    /**
     * Frees an id for this store's {@link IdGenerator}.
     *
     * @param id The id to free
     */
    public void freeId( long id )
    {
        idGenerator.freeId( id );
    }

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
     * @param highId The high id to set.
     */
    public void setHighId( long highId )
    {
        if ( idGenerator != null )
        {
            idGenerator.setHighId( highId );
        }
    }

    /**
     * If store is not ok a call to this method will rebuild the {@link
     * IdGenerator} used by this store and if successful mark it as
     * <CODE>ok</CODE>.
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
            causeOfStoreNotOk = null;
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
     * @return the store directory from config.
     */
    protected File getStoreDir()
    {
        return configuration.get( Configuration.store_dir );
    }

    /**
     * Acquires a {@link PersistenceWindow} for <CODE>position</CODE> and
     * operation <CODE>type</CODE>. Window must be released after operation
     * has been performed via {@link #releaseWindow(PersistenceWindow)}.
     *
     * @param position The record position
     * @param type     The operation type
     * @return a persistence window encapsulating the record
     */
    protected PersistenceWindow acquireWindow( long position, OperationType type )
    {
        if ( !isInRecoveryMode() && (position > getHighId() || !storeOk) )
        {
            throw new InvalidRecordException(
                    "Position[" + position + "] requested for high id[" + getHighId() + "], store is ok[" + storeOk +
                    "] recovery[" + isInRecoveryMode() + "]", causeOfStoreNotOk );
        }
        return windowPool.acquire( position, type );
    }

    /**
     * Releases the window and writes the data (async) if the
     * <CODE>window</CODE> was a {@link PersistenceRow}.
     *
     * @param window The window to be released
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

    public boolean isInRecoveryMode()
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
    public File getStorageFileName()
    {
        return storageFileName;
    }

    /** Opens the {@link IdGenerator} used by this store. */
    protected void openIdGenerator()
    {
        idGenerator = openIdGenerator( new File( storageFileName.getPath() + ".id" ), idType.getGrabSize() );

        /* MP: 2011-11-23
         * There may have been some migration done in the startup process, so if there have been some
         * high id registered during, then update id generators. updateHighId does nothing if
         * not registerIdFromUpdateRecord have been called.
         */
        updateHighId();
    }

    protected IdGenerator openIdGenerator( File fileName, int grabSize )
    {
        return idGeneratorFactory.open( fileSystemAbstraction, fileName, grabSize,
                getIdType(), findHighIdBackwards() );
    }

    protected long findHighIdBackwards()
    {
        try
        {
            StoreChannel fileChannel = getFileChannel();
            int recordSize = getRecordSize();
            long fileSize = fileChannel.size();
            long highId = fileSize / recordSize;
            ByteBuffer byteBuffer = ByteBuffer.allocate( bytesRequiredToDetermineInUse() );
            for ( long i = highId; i >= 0; i-- )
            {
                fileChannel.position( i * recordSize );
                if ( fileChannel.read( byteBuffer ) > 0 )
                {
                    byteBuffer.flip();
                    boolean isInUse = isRecordInUse( byteBuffer );
                    byteBuffer.clear();
                    if ( isInUse )
                    {
                        return i+1;
                    }
                }
            }
            return getNumberOfReservedLowIds();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Unable to rebuild id generator " + getStorageFileName(), e );
        }
    }

    public abstract int getRecordSize();

    protected abstract boolean isRecordInUse( ByteBuffer recordData );

    protected int bytesRequiredToDetermineInUse()
    {   // For most records it's enough with looking at the first byte
        return 1;
    }

    protected boolean isRecordReserved( ByteBuffer recordData )
    {
        return false;
    }

    protected void createIdGenerator( File fileName )
    {
        idGeneratorFactory.create( fileSystemAbstraction, fileName, 0 );
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

    /** Closed the {@link IdGenerator} used by this store */
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
     */
    @Override
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
        if ( (isReadOnly() && !isBackupSlave()) || idGenerator == null || !storeOk )
        {
            releaseFileLockAndCloseFileChannel();
            return;
        }
        final long highId = idGenerator.getHighId();
        final int recordSize = getEffectiveRecordSize();
        idGenerator.close();
        IOException storedIoe = null;
        // hack for WINBLOWS
        if ( !readOnly || backupSlave )
        {
            try
            {
                windowsSafeIOOperation( new FileOperation()
                {
                    @Override
                    public void perform() throws IOException
                    {
                        fileChannel.position( highId * recordSize );
                        ByteBuffer buffer = wrap( encode( getTypeAndVersionDescriptor() ) );
                        fileChannel.write( buffer );
                        stringLogger.debug( "Closing " + storageFileName + ", truncating at " + fileChannel.position() +
                                            " vs file size " + fileChannel.size() );
                        fileChannel.truncate( fileChannel.position() );
                        fileChannel.force( false );
                        releaseFileLockAndCloseFileChannel();
                    }
                } );
            }
            catch ( IOException e )
            {
                storedIoe = e;
            }
        }
        else
        {
            releaseFileLockAndCloseFileChannel();
        }

        if ( storedIoe != null )
        {
            throw new UnderlyingStorageException( "Unable to close store " + getStorageFileName(), storedIoe );
        }
    }

    protected void releaseFileLockAndCloseFileChannel()
    {
        try
        {
            if ( fileLock != null )
            {
                fileLock.release();
            }
            if ( fileChannel != null )
            {
                fileChannel.close();
            }
        }
        catch ( IOException e )
        {
            stringLogger.warn( "Could not close [" + storageFileName + "]", e );
        }
        fileChannel = null;
    }

    /**
     * Returns a <CODE>StoreChannel</CODE> to this storage's file. If
     * <CODE>close()</CODE> method has been invoked <CODE>null</CODE> will be
     * returned.
     *
     * @return A file channel to this storage
     */
    protected final StoreChannel getFileChannel()
    {
        return fileChannel;
    }

    /** @return The highest possible id in use, -1 if no id in use. */
    public long getHighestPossibleIdInUse()
    {
        if ( idGenerator != null )
        {
            return idGenerator.getHighId() - 1;
        }
        else
        {   // If we ask for this before we've recovered we can only make a best-effort guess
            // about the highest possible id in use.
            return calculateHighestIdInUseByLookingAtFileSize();
        }
    }

    private long calculateHighestIdInUseByLookingAtFileSize()
    {
        try
        {
            return getFileChannel().size() / getRecordSize();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /** @return The total number of ids in use. */
    public long getNumberOfIdsInUse()
    {
        return idGenerator.getNumberOfIdsInUse();
    }

    /**
     * @return the number of records at the beginning of the store file that are reserved for other things
     * than actual records. Stuff like permanent configuration data.
     */
    public int getNumberOfReservedLowIds()
    {
        return 0;
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
        if ( isInRecoveryMode() )
        {
            highestUpdateRecordId = Math.max( highestUpdateRecordId, id + 1 );
        }
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

    public void logVersions( StringLogger.LineLogger logger )
    {
        logger.logLine( "  " + getTypeAndVersionDescriptor() );
    }

    public void logIdUsage( StringLogger.LineLogger lineLogger )
    {
        lineLogger.logLine( String.format( "  %s: used=%s high=%s",
                                           getTypeDescriptor(), getNumberOfIdsInUse(), getHighestPossibleIdInUse() ) );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
