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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils.FileOperation;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

import static java.nio.ByteBuffer.wrap;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.io.fs.FileUtils.windowsSafeIOOperation;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

/**
 * Contains common implementation for {@link AbstractStore} and
 * {@link AbstractDynamicStore}.
 */
public abstract class CommonAbstractStore implements IdSequence
{
    public static abstract class Configuration
    {
        public static final Setting<File> store_dir = InternalAbstractGraphDatabase.Configuration.store_dir;
        public static final Setting<File> neo_store = InternalAbstractGraphDatabase.Configuration.neo_store;
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
    }

    public static final String ALL_STORES_VERSION = "v0.A.4";
    public static final String UNKNOWN_VERSION = "Unknown";

    protected final Config configuration;
    private final IdGeneratorFactory idGeneratorFactory;
    protected final PageCache pageCache;
    protected FileSystemAbstraction fileSystemAbstraction;
    protected final File storageFileName;
    protected final IdType idType;
    protected StringLogger stringLogger;
    private IdGenerator idGenerator;
    private StoreChannel fileChannel;
    protected PagedFile storeFile;
    private boolean storeOk = true;
    private Throwable causeOfStoreNotOk;
    private FileLock fileLock;
    private final boolean readOnly;
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
     * @param monitors
     */
    public CommonAbstractStore(
            File fileName,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger,
            StoreVersionMismatchHandler versionMismatchHandler,
            Monitors monitors )
    {
        this.storageFileName = fileName;
        this.configuration = configuration;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.idType = idType;
        this.stringLogger = stringLogger;
        this.versionMismatchHandler = versionMismatchHandler;
        this.readOnly = configuration.get( Configuration.read_only );

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

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    /**
     * Returns the type and version that identifies this store.
     *
     * @return This store's implementation type and version identifier
     */
    public abstract String getTypeDescriptor();

    /**
     * Note: This method runs before the file has been mapped by the page cache, and therefore needs to
     * operate on the store files directly. This method is called by constructors.
     */
    protected void checkStorage()
    {
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
            if ( !readOnly )
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

    /**
     * Note: This method runs before the file has been mapped by the page cache, and therefore needs to
     * operate on the store files directly. This method is called by constructors.
     */
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
     *
     * Note: This method will map the file with the page cache. The store file must not
     * be accessed directly until it has been unmapped - the store file must only be
     * accessed through the page cache.
     */
    protected void loadStorage()
    {
        try
        {
            readAndVerifyBlockSize();
            verifyFileSizeAndTruncate();
            loadIdGenerator();
            try
            {
                int filePageSize = pageCache.pageSize() - pageCache.pageSize() % getEffectiveRecordSize();
                storeFile = pageCache.map( getStorageFileName(), filePageSize );
            }
            catch ( IOException e )
            {
                // TODO: Just throw IOException, add proper handling further up
                throw new UnderlyingStorageException( e );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to load storage " + getStorageFileName(), e );
        }
    }

    protected long pageIdForRecord( long id )
    {
        return id * getEffectiveRecordSize() / storeFile.pageSize();
    }

    protected int offsetForId( long id )
    {
        return (int) (id * getEffectiveRecordSize() % storeFile.pageSize());
    }

    protected int recordsPerPage()
    {
        return storeFile.pageSize() / getEffectiveRecordSize();
    }

    protected abstract int getEffectiveRecordSize();

    /**
     * Note: This method runs before the file has been mapped by the page cache, and therefore needs to
     * operate on the store files directly. This method is called by constructors.
     */
    protected abstract void verifyFileSizeAndTruncate() throws IOException;

    /**
     * Note: This method runs before the file has been mapped by the page cache, and therefore needs to
     * operate on the store files directly. This method is called by constructors.
     */
    protected abstract void readAndVerifyBlockSize() throws IOException;

    private void loadIdGenerator()
    {
        try
        {
            if ( !isReadOnly() )
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
                    stringLogger.logMessage( getStorageFileName() + " non clean shutdown detected", true );
                }
            }
        }
    }

    /**
     * Note: This method runs before the file has been mapped by the page cache, and therefore needs to
     * operate on the store files directly. This method is called by constructors.
     */
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

    protected abstract boolean isInUse( byte inUseByte );

    protected abstract boolean useFastIdGeneratorRebuilding();

    protected abstract boolean firstRecordIsHeader();

    /**
     * Should rebuild the id generator from scratch.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementors must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected void rebuildIdGenerator()
    {
        int blockSize = getEffectiveRecordSize();
        if ( blockSize <= 0 )
        {
            throw new InvalidRecordException( "Illegal blockSize: " + blockSize );
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
        if ( firstRecordIsHeader() )
        {
            setHighId( 1 ); // reserved first block containing blockSize
        }

        int filePageSize = storeFile.pageSize();
        int recordsPerPage = recordsPerPage();
        long defraggedCount;

        try
        {
            PagedFile pagedFile = pageCache.map( storageFileName, filePageSize );
            try
            {
                if ( useFastIdGeneratorRebuilding() )
                {
                    try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
                    {
                        defraggedCount = rebuildIdGeneratorFast( cursor, recordsPerPage, blockSize, pagedFile.getLastPageId() );
                    }
                }
                else
                {
                    try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK | PF_READ_AHEAD ) )
                    {
                        defraggedCount = rebuildIdGeneratorSlow( cursor, recordsPerPage, blockSize );
                    }
                }
            }
            finally
            {
                pageCache.unmap( storageFileName );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to rebuild id generator " + getStorageFileName(), e );
        }

        stringLogger.debug( "[" + getStorageFileName() + "] high id=" + getHighId()
                + " (defragged=" + defraggedCount + ")" );
        stringLogger.logMessage( getStorageFileName() + " rebuild id generator, highId=" + getHighId() +
                " defragged count=" + defraggedCount, true );
        closeIdGenerator();
        openIdGenerator();
    }

    private long rebuildIdGeneratorFast( PageCursor cursor, int recordsPerPage, int blockSize, long lastPageId ) throws IOException
    {
        long nextPageId = lastPageId;

        while ( nextPageId >= 0 && cursor.next( nextPageId ) )
        {
            nextPageId--;
            do {
                int currentRecord = recordsPerPage;
                while ( currentRecord --> 0 )
                {
                    cursor.setOffset( currentRecord * blockSize );
                    byte inUseByte = cursor.getByte();

                    if ( isInUse( inUseByte ) )
                    {
                        // We've found the highest id that is in use
                        setHighId( (cursor.getCurrentPageId() * recordsPerPage) + currentRecord );
                        // Return the number of records that we skipped over to find it
                        return ((lastPageId - nextPageId) * recordsPerPage) - currentRecord;
                    }
                }
            } while ( cursor.shouldRetry() );
        }

        return 0;
    }

    private long rebuildIdGeneratorSlow( PageCursor cursor, int recordsPerPage, int blockSize ) throws IOException
    {
        long defragCount = 0;
        long[] freedBatch = new long[recordsPerPage]; // we process in batches of one page worth of records
        int startingId = firstRecordIsHeader()? 1 : 0;
        int defragged;

        while ( cursor.next() )
        {
            long idPageOffset = (cursor.getCurrentPageId() * recordsPerPage);

            do {
                defragged = 0;
                for ( int i = startingId; i < recordsPerPage; i++ )
                {
                    cursor.setOffset( i * blockSize );
                    byte inUseByte = cursor.getByte();

                    long recordId = idPageOffset + i;
                    if ( !isInUse( inUseByte ) )
                    {
                        freedBatch[defragged] = recordId;
                        defragged++;
                    }
                }
            } while ( cursor.shouldRetry() );

            setHighId( idPageOffset + recordsPerPage );
            for ( int i = 0; i < defragged; i++ )
            {
                long freedId = freedBatch[i];
                freeId( freedId );
            }
            defragCount += defragged;
            startingId = 0;
        }

        return defragCount;
    }

    /**
     * This method should close/release all resources that the implementation of
     * this store has allocated and is called just before the <CODE>close()</CODE>
     * method returns. Override this method to clean up stuff the constructor.
     * <p>
     * This default implementation does nothing.
     * <p>
     * Note: This method runs before the store file is unmapped from the page cache,
     * and is therefore not allowed to operate on the store files directly.
     */
    protected void closeStorage()
    {
    }

    boolean isReadOnly()
    {
        return readOnly;
    }

    /**
     * Marks this store as "not ok".
     */
    protected void setStoreNotOk( Throwable cause )
    {
        if ( readOnly )
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
    @Override
    public long nextId()
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
        return idGenerator != null ? idGenerator.getHighId() : -1;
    }

    /**
     * Sets the high id, i.e. highest id in use + 1 (use this when rebuilding id generator).
     *
     * @param highId The high id to set.
     */
    public void setHighId( long highId )
    {
        // This method might get called during recovery, where we don't have a reliable id generator yet,
        // so ignore these calls and let rebuildIdGenerators() figure out the high id after recovery.
        if ( idGenerator != null )
        {
            synchronized ( idGenerator )
            {
                if ( highId > idGenerator.getHighId() )
                {
                    idGenerator.setHighId( highId );
                }
            }
        }
    }

    /**
     * Sets the highest id in use. After this call highId will be this given id + 1.
     *
     * @param highId The highest id in use to set.
     */
    public void setHighestPossibleIdInUse( long highId )
    {
        setHighId( highId+1 );
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
            if ( readOnly )
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
        if ( readOnly )
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

    protected void assertIdExists( long position )
    {
        if ( !isInRecoveryMode() && (position > getHighId() || !storeOk) )
        {
            throw new InvalidRecordException(
                    "Position[" + position + "] requested for high id[" + getHighId() + "], store is ok[" + storeOk +
                    "] recovery[" + isInRecoveryMode() + "]", causeOfStoreNotOk );
        }
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

    /**
     * Opens the {@link IdGenerator} used by this store.
     *
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementors must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected void openIdGenerator()
    {
        idGenerator = openIdGenerator( new File( storageFileName.getPath() + ".id" ), idType.getGrabSize() );
    }

    /**
     * Opens the {@link IdGenerator} given by the fileName.
     *
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementors must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected IdGenerator openIdGenerator( File fileName, int grabSize )
    {
        IdType type = getIdType();
        long highestIdInUse = figureOutHighestIdInUse();
        return idGeneratorFactory.open( fileSystemAbstraction, fileName, grabSize, type, highestIdInUse );
    }

    /**
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementors must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected abstract long figureOutHighestIdInUse();

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

    public void flush()
    {
        try
        {
            storeFile.flush();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
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
    public void close()
    {
        if ( fileChannel == null )
        {
            return;
        }
        closeStorage();
        try
        {
            pageCache.unmap( getStorageFileName() );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to close store file: " + getStorageFileName(), e );
        }
        if ( isReadOnly() || idGenerator == null || !storeOk )
        {
            releaseFileLockAndCloseFileChannel();
            return;
        }
        final long highId = idGenerator.getHighId();
        final int recordSize = getEffectiveRecordSize();
        idGenerator.close();
        IOException storedIoe = null;
        // hack for WINBLOWS
        if ( !readOnly )
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
     * <p>
     * Note: You can only operate directly on the StoreChannel while the file
     * is not mapped in the page cache.
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
            return idGenerator.getHighestPossibleIdInUse();
        }
        
        // If we ask for this before we've recovered we can only make a best-effort guess
        // about the highest possible id in use.
        return figureOutHighestIdInUse();
    }

    /** @return The total number of ids in use. */
    public long getNumberOfIdsInUse()
    {
        return idGenerator.getNumberOfIdsInUse();
    }

    public IdType getIdType()
    {
        return idType;
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
