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

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.store.StoreVersionTrailerUtil.getTrailerOffset;
import static org.neo4j.kernel.impl.store.StoreVersionTrailerUtil.writeTrailer;

/**
 * Contains common implementation for {@link AbstractStore} and
 * {@link AbstractDynamicStore}.
 */
public abstract class CommonAbstractStore implements IdSequence, AutoCloseable
{
    public static final String ALL_STORES_VERSION = "v0.A.6";
    public static final String UNKNOWN_VERSION = "Unknown";
    protected final Config configuration;
    protected final PageCache pageCache;
    protected final File storageFileName;
    protected final IdType idType;
    private final IdGeneratorFactory idGeneratorFactory;
    private final StoreVersionMismatchHandler versionMismatchHandler;
    protected final Log log;
    protected PagedFile storeFile;
    private IdGenerator idGenerator;
    private boolean storeOk = true;
    private Throwable causeOfStoreNotOk;
    private String readTypeDescriptorAndVersion;

    /**
     * Opens and validates the store contained in <CODE>fileName</CODE>
     * loading any configuration defined in <CODE>config</CODE>. After
     * validation the <CODE>initStorage</CODE> method is called.
     * <p>
     * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>
     * and the {@link #getStoreOk()} method will return true.
     * If a problem was found when opening the store the {@link #makeStoreOk()}
     * must be invoked.
     * <p>
     * throws IOException if the unable to open the storage or if the
     * <CODE>initStorage</CODE> method fails
     *
     * @param idType The Id used to index into this store
     */
    public CommonAbstractStore(
            File fileName,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            StoreVersionMismatchHandler versionMismatchHandler )
    {
        this.storageFileName = fileName;
        this.configuration = configuration;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.idType = idType;
        this.log = logProvider.getLog( getClass() );
        this.versionMismatchHandler = versionMismatchHandler;

        try
        {
            checkStorage();
            checkVersion(); // Overriden in NeoStore
            loadStorage();
        }
        catch ( Exception e )
        {
            if ( storeFile != null )
            {
                try
                {
                    storeFile.close();
                }
                catch ( IOException failureToClose )
                {
                    // Not really a suppressed exception, but we still want to throw the real exception, e,
                    // but perhaps also throw this in there or convenience.
                    e.addSuppressed( failureToClose );
                }
            }
            throw launderedException( e );
        }
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

    public String getTypeAndVersionDescriptor()
    {
        return buildTypeDescriptorAndVersion( getTypeDescriptor() );
    }

    /**
     * Returns the type and version that identifies this store.
     *
     * @return This store's implementation type and version identifier
     */
    public abstract String getTypeDescriptor();

    /**
     * This method is called by constructors.
     */
    protected void checkStorage()
    {
        try ( PagedFile file = pageCache.map( storageFileName, pageCache.pageSize() ) )
        {
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open file " + storageFileName, e );
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
     * <p>
     * Note: This method will map the file with the page cache. The store file must not
     * be accessed directly until it has been unmapped - the store file must only be
     * accessed through the page cache.
     */
    protected void loadStorage()
    {
        try
        {
            readAndVerifyBlockSize();
            verifyRecordAlignmentAndRemoveTrailer();
            try
            {
                int filePageSize = pageCache.pageSize() - pageCache.pageSize() % getRecordSize();
                storeFile = pageCache.map( getStorageFileName(), filePageSize );
            }
            catch ( IOException e )
            {
                // TODO: Just throw IOException, add proper handling further up
                throw new UnderlyingStorageException( e );
            }
            loadIdGenerator();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to load storage " + getStorageFileName(), e );
        }
    }

    private void verifyRecordAlignmentAndRemoveTrailer() throws IOException
    {
        String versionTrailer = buildTypeDescriptorAndVersion( getTypeDescriptor() );
        int expectedVersionLength = UTF8.encode( versionTrailer ).length;
        try ( PagedFile pagedFile = pageCache
                .map( getStorageFileName(), pageCache.pageSize() - pageCache.pageSize() % getRecordSize() ) )
        {
            long trailerOffset =
                    getTrailerOffset( pagedFile, versionTrailer.split( " " )[0] );
            if ( trailerOffset != -1 && trailerOffset % getRecordSize() == 0 )
            {
                writeTrailer( pagedFile, new byte[expectedVersionLength], trailerOffset );
            }
            else
            {
                setStoreNotOk( new IllegalStateException(
                        "Misaligned file size " + trailerOffset + " for " + this + ", expected version length:" +
                        expectedVersionLength ) );
            }
        }
    }

    protected long pageIdForRecord( long id )
    {
        return id * getRecordSize() / storeFile.pageSize();
    }

    protected int offsetForId( long id )
    {
        return (int) (id * getRecordSize() % storeFile.pageSize());
    }

    public int getRecordsPerPage()
    {
        return storeFile.pageSize() / getRecordSize();
    }

    /**
     * Note: This method runs before the file has been mapped by the page cache, and therefore needs to
     * operate on the store files directly. This method is called by constructors.
     */
    protected abstract void readAndVerifyBlockSize() throws IOException;

    private void loadIdGenerator()
    {
        try
        {
            if ( storeOk )
            {
                openIdGenerator();
            }
            // else we will rebuild the id generator after recovery, and we don't want to have the id generator
            // picking up calls to freeId during recovery.
        }
        catch ( InvalidIdGeneratorException e )
        {
            setStoreNotOk( e );
        }
        finally
        {
            if ( !getStoreOk() )
            {
                log.debug( getStorageFileName() + " non clean shutdown detected" );
            }
        }
    }

    protected void verifyCorrectTypeDescriptorAndVersion() throws IOException
    {
        String expectedTypeDescriptorAndVersion = getTypeAndVersionDescriptor();
        try ( PagedFile pagedFile = pageCache.map( storageFileName, pageCache.pageSize() ) )
        {
            readTypeDescriptorAndVersion = StoreVersionTrailerUtil
                    .readTrailer( pagedFile, expectedTypeDescriptorAndVersion );
        }
        if ( readTypeDescriptorAndVersion == null )
        {
            setStoreNotOk( new IllegalStateException(
                    "No trailer present in store, expected " + expectedTypeDescriptorAndVersion ) );
            return;
        }
        if ( !expectedTypeDescriptorAndVersion.equals( readTypeDescriptorAndVersion ) )
        {
            if ( readTypeDescriptorAndVersion.startsWith( getTypeDescriptor() ) )
            {
                versionMismatchHandler.mismatch( expectedTypeDescriptorAndVersion, readTypeDescriptorAndVersion );
            }
            else
            {
                setStoreNotOk( new IllegalStateException( "Unexpected version " + readTypeDescriptorAndVersion +
                                                          ", expected " + expectedTypeDescriptorAndVersion ) );
            }
        }
    }

    protected int getHeaderRecord() throws IOException
    {
        int headerRecord = 0 ;
        try ( PagedFile pagedFile = pageCache
                .map( getStorageFileName(), pageCache.pageSize() ) )
        {
            try ( PageCursor pageCursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
            {
                if ( pageCursor.next() )
                {
                    do
                    {
                        headerRecord = pageCursor.getInt();
                    }
                    while ( pageCursor.shouldRetry() );
                }
            }
        }
        if ( headerRecord <= 0 )
        {
            throw new InvalidRecordException( "Illegal block size: " +
                    headerRecord + " in " + getStorageFileName() );
        }
        return headerRecord;
    }

    protected abstract boolean isInUse( byte inUseByte );

    /**
     * Should rebuild the id generator from scratch.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected void rebuildIdGenerator()
    {
        int blockSize = getRecordSize();
        if ( blockSize <= 0 )
        {
            throw new InvalidRecordException( "Illegal blockSize: " + blockSize );
        }

        log.debug( "Rebuilding id generator for[" + getStorageFileName() + "] ..." );
        closeIdGenerator();
        createIdGenerator( getIdFileName() );
        openIdGenerator();

        long defraggedCount = 0;
        boolean fastRebuild = doFastIdGeneratorRebuild();

        try
        {
            long foundHighId = scanForHighId();
            setHighId( foundHighId );
            if ( !fastRebuild )
            {
                try ( PageCursor cursor = storeFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK | PF_READ_AHEAD ) )
                {
                    defraggedCount = rebuildIdGeneratorSlow( cursor, getRecordsPerPage(), blockSize,
                            foundHighId );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to rebuild id generator " + getStorageFileName(), e );
        }

        log.debug( "[" + getStorageFileName() + "] high id=" + getHighId()
                            + " (defragged=" + defraggedCount + ")" );
        log.debug( getStorageFileName() + " rebuild id generator, highId=" + getHighId() +
                                 " defragged count=" + defraggedCount );

        if ( !fastRebuild )
        {
            closeIdGenerator();
            openIdGenerator();
        }
    }

    private long rebuildIdGeneratorSlow( PageCursor cursor, int recordsPerPage, int blockSize,
                                         long foundHighId )
            throws IOException
    {
        long defragCount = 0;
        long[] freedBatch = new long[recordsPerPage]; // we process in batches of one page worth of records
        int startingId = getNumberOfReservedLowIds();
        int defragged;

        boolean done = false;
        while ( !done && cursor.next() )
        {
            long idPageOffset = (cursor.getCurrentPageId() * recordsPerPage);

            do
            {
                defragged = 0;
                done = false;
                for ( int i = startingId; i < recordsPerPage; i++ )
                {
                    int offset = i * blockSize;
                    cursor.setOffset( offset );
                    long recordId = idPageOffset + i;
                    if ( recordId >= foundHighId )
                    {   // We don't have to go further than the high id we found earlier
                        done = true;
                        break;
                    }

                    if ( !isRecordInUse( cursor ) )
                    {
                        freedBatch[defragged++] = recordId;
                    }
                    else if ( isRecordReserved( cursor ) )
                    {
                        cursor.setOffset( offset );
                        cursor.putByte( Record.NOT_IN_USE.byteValue() );
                        cursor.putInt( 0 );
                        freedBatch[defragged++] = recordId;
                    }
                }
            }
            while ( cursor.shouldRetry() );

            for ( int i = 0; i < defragged; i++ )
            {
                freeId( freedBatch[i] );
            }
            defragCount += defragged;
            startingId = 0;
        }
        return defragCount;
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
     * <p>
     * Note: This method runs before the store file is unmapped from the page cache.
     */
    protected void closeStorage()
    {
    }

    /**
     * Marks this store as "not ok".
     */
    protected void setStoreNotOk( Throwable cause )
    {
        storeOk = false;
        causeOfStoreNotOk = cause;
        idGenerator = null; // since we will rebuild it later
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
     * Throws cause of not being OK if {@link #getStoreOk()} returns {@code false}.
     */
    protected void checkStoreOk()
    {
        if ( !storeOk )
        {
            throw new UnderlyingStorageException( "Store is not OK", causeOfStoreNotOk );
        }
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
        if ( idGenerator != null )
        {
            idGenerator.freeId( id );
        }
        // else we're deleting records as part of applying transactions during recovery, and that's fine
    }

    /**
     * Return the highest id in use.
     * If this store is not OK yet, the high id is calculated from the highest in use record on the store,
     * using {@link #scanForHighId()}.
     *
     * @return The high id, highest id in use + 1
     */
    public long getHighId()
    {
        return idGenerator != null ? idGenerator.getHighId() : scanForHighId();
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
     * If store is not ok a call to this method will rebuild the {@link
     * IdGenerator} used by this store and if successful mark it as.
     *
     * WARNING: this method must NOT be called if recovery is required, but hasn't performed.
     * To remove all negations from the above statement: Only call this method if store is in need of
     * recovery and recovery has been performed.
     */
    public void makeStoreOk()
    {
        if ( !storeOk )
        {
            rebuildIdGenerator();
            storeOk = true;
            causeOfStoreNotOk = null;
        }
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

    private File getIdFileName()
    {
        return new File( getStorageFileName().getPath() + ".id" );
    }

    /**
     * Opens the {@link IdGenerator} used by this store.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected void openIdGenerator()
    {
        idGenerator = openIdGenerator( getIdFileName(), idType.getGrabSize() );
    }

    /**
     * Opens the {@link IdGenerator} given by the fileName.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected IdGenerator openIdGenerator( File fileName, int grabSize )
    {
        return idGeneratorFactory.open( fileName, grabSize, getIdType(), scanForHighId() );
    }

    /**
     * Starts from the end of the file and scans backwards to find the highest in use record.
     * Can be used even if {@link #makeStoreOk()} hasn't been called. Basically this method should be used
     * over {@link #getHighestPossibleIdInUse()} and {@link #getHighId()} in cases where a store has been opened
     * but is in a scenario where recovery isn't possible, like some tooling or migration.
     *
     * @return the id of the highest in use record + 1, i.e. highId.
     */
    protected long scanForHighId()
    {
        try ( PageCursor cursor = storeFile.io( 0, PF_SHARED_LOCK ) )
        {
            long nextPageId = storeFile.getLastPageId();
            int recordsPerPage = getRecordsPerPage();
            int recordSize = getRecordSize();
            while ( nextPageId >= 0 && cursor.next( nextPageId ) )
            {
                nextPageId--;
                do
                {
                    int currentRecord = recordsPerPage;
                    while ( currentRecord-- > 0 )
                    {
                        cursor.setOffset( currentRecord * recordSize );
                        long recordId = (cursor.getCurrentPageId() * recordsPerPage) + currentRecord;
                        if ( isRecordInUse( cursor ) )
                        {
                            // We've found the highest id in use
                            return recordId + 1 /*+1 since we return the high id*/;
                        }
                    }
                }
                while ( cursor.shouldRetry() );
            }

            return getNumberOfReservedLowIds();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to find high id by scanning backwards " + getStorageFileName(), e );
        }
    }

    public abstract int getRecordSize();

    protected boolean isRecordInUse( PageCursor cursor )
    {
        return isInUse( cursor.getByte() );
    }

    protected boolean isRecordReserved( PageCursor cursor )
    {
        return false;
    }

    protected void createIdGenerator( File fileName )
    {
        idGeneratorFactory.create( fileName, 0, false );
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
            storeFile.flushAndForce();
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
     * before the pagedFile is closed.
     */
    @Override
    public void close()
    {
        closeStorage();
        if ( idGenerator == null || !storeOk )
        {
            try
            {
                storeFile.close();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Failed to close store file: " + getStorageFileName(), e );
            }
            return;
        }
        try
        {
            long trailerOffset = idGenerator.getHighId() * getRecordSize();
            idGenerator.close();

            log.debug( "Closing " + storageFileName + ", writing trailer at " + trailerOffset );
            writeTrailer( storeFile, encode( versionMismatchHandler.trailerToWrite( getTypeAndVersionDescriptor(),
                    readTypeDescriptorAndVersion ) ), trailerOffset );
            storeFile.close();
        }
        catch ( IOException | IllegalStateException e )
        {
            throw new UnderlyingStorageException( "Failed to close store file: " + getStorageFileName(), e );
        }
    }

    /** @return The highest possible id in use, -1 if no id in use. */
    public long getHighestPossibleIdInUse()
    {
        if ( idGenerator != null )
        {
            return idGenerator.getHighestPossibleIdInUse();
        }
        else
        {   // If we ask for this before we've recovered we can only make a best-effort guess
            // about the highest possible id in use.
            return scanForHighId() - 1;
        }
    }

    /**
     * Sets the highest id in use. After this call highId will be this given id + 1.
     *
     * @param highId The highest id in use to set.
     */
    public void setHighestPossibleIdInUse( long highId )
    {
        setHighId( highId + 1 );
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

    public IdType getIdType()
    {
        return idType;
    }

    public void logVersions( Logger logger )
    {
        logger.log( "  " + getTypeAndVersionDescriptor() );
    }

    public void logIdUsage( Logger logger )
    {
        logger.log( String.format( "  %s: used=%s high=%s",
                getTypeDescriptor(), getNumberOfIdsInUse(), getHighestPossibleIdInUse() ) );
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #makeStoreOk()},
     * {@link #closeStorage()} (where that method could be deleted all together and do a visit in {@link #close()}),
     * {@link #logIdUsage(Logger)},
     * {@link #logVersions(Logger)}
     * For a good samaritan to pick up later.
     */
    public void visitStore( Visitor<CommonAbstractStore,RuntimeException> visitor )
    {
        visitor.visit( this );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    public static abstract class Configuration
    {
        public static final Setting<Boolean> rebuild_idgenerators_fast =
                GraphDatabaseSettings.rebuild_idgenerators_fast;
    }
}
