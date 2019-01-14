/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static java.lang.Math.max;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.neo4j.helpers.ArrayUtil.contains;
import static org.neo4j.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.ANY_PAGE_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Contains common implementation of {@link RecordStore}.
 */
public abstract class CommonAbstractStore<RECORD extends AbstractBaseRecord,HEADER extends StoreHeader>
        implements RecordStore<RECORD>, AutoCloseable
{
    static final String UNKNOWN_VERSION = "Unknown";

    protected final Config configuration;
    protected final PageCache pageCache;
    final File storageFileName;
    protected final IdType idType;
    protected final IdGeneratorFactory idGeneratorFactory;
    protected final Log log;
    protected PagedFile storeFile;
    protected final String storeVersion;
    protected final RecordFormat<RECORD> recordFormat;
    private IdGenerator idGenerator;
    private boolean storeOk = true;
    private RuntimeException causeOfStoreNotOk;
    private final String typeDescriptor;
    protected int recordSize;

    private final StoreHeaderFormat<HEADER> storeHeaderFormat;
    private HEADER storeHeader;

    private final OpenOption[] openOptions;

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
            String typeDescriptor,
            RecordFormat<RECORD> recordFormat,
            StoreHeaderFormat<HEADER> storeHeaderFormat,
            String storeVersion,
            OpenOption... openOptions )
    {
        this.storageFileName = fileName;
        this.configuration = configuration;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.idType = idType;
        this.typeDescriptor = typeDescriptor;
        this.recordFormat = recordFormat;
        this.storeHeaderFormat = storeHeaderFormat;
        this.storeVersion = storeVersion;
        this.openOptions = openOptions;
        this.log = logProvider.getLog( getClass() );
    }

    void initialise( boolean createIfNotExists )
    {
        try
        {
            checkAndLoadStorage( createIfNotExists );
        }
        catch ( Exception e )
        {
            closeAndThrow( e );
        }
    }

    private void closeAndThrow( Exception e )
    {
        if ( storeFile != null )
        {
            try
            {
                closeStoreFile();
            }
            catch ( IOException failureToClose )
            {
                // Not really a suppressed exception, but we still want to throw the real exception, e,
                // but perhaps also throw this in there or convenience.
                e.addSuppressed( failureToClose );
            }
        }
        throwIfUnchecked( e );
        throw new RuntimeException( e );
    }

    /**
     * Returns the type and version that identifies this store.
     *
     * @return This store's implementation type and version identifier
     */
    public String getTypeDescriptor()
    {
        return typeDescriptor;
    }

    /**
     * This method is called by constructors. Checks the header record and loads the store.
     * <p>
     * Note: This method will map the file with the page cache. The store file must not
     * be accessed directly until it has been unmapped - the store file must only be
     * accessed through the page cache.
     * @param createIfNotExists If true, creates and initialises the store file if it does not exist already. If false,
     * this method will instead throw an exception in that situation.
     */
    protected void checkAndLoadStorage( boolean createIfNotExists )
    {
        int pageSize = pageCache.pageSize();
        int filePageSize;
        try ( PagedFile pagedFile = pageCache.map( storageFileName, pageSize, ANY_PAGE_SIZE ) )
        {
            extractHeaderRecord( pagedFile );
            filePageSize = pageCache.pageSize() - pageCache.pageSize() % getRecordSize();
        }
        catch ( NoSuchFileException | StoreNotFoundException e )
        {
            if ( createIfNotExists )
            {
                try
                {
                    createStore( pageSize );
                    return;
                }
                catch ( IOException e1 )
                {
                    e.addSuppressed( e1 );
                }
            }
            if ( e instanceof StoreNotFoundException )
            {
                throw (StoreNotFoundException) e;
            }
            throw new StoreNotFoundException( "Store file not found: " + storageFileName, e );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open store file: " + storageFileName, e );
        }
        loadStorage( filePageSize );
    }

    private void createStore( int pageSize ) throws IOException
    {
        try ( PagedFile file = pageCache.map( storageFileName, pageSize, StandardOpenOption.CREATE ) )
        {
            initialiseNewStoreFile( file );
        }
        checkAndLoadStorage( false );
    }

    private void loadStorage( int filePageSize )
    {
        try
        {
            storeFile = pageCache.map( getStorageFileName(), filePageSize, openOptions );
            loadIdGenerator();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open store file: " + storageFileName, e );
        }
    }

    protected void initialiseNewStoreFile( PagedFile file ) throws IOException
    {
        if ( getNumberOfReservedLowIds() > 0 )
        {
            try ( PageCursor pageCursor = file.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                if ( pageCursor.next() )
                {
                    pageCursor.setOffset( 0 );
                    createHeaderRecord( pageCursor );
                    if ( pageCursor.checkAndClearBoundsFlag() )
                    {
                        throw new UnderlyingStorageException(
                                "Out of page bounds when writing header; page size too small: " + pageCache.pageSize() +
                                        " bytes." );
                    }
                }
            }
        }

        // Determine record size right after writing the header since some stores
        // use it when initializing their stores to write some records.
        recordSize = determineRecordSize();

        idGeneratorFactory.create( getIdFileName(), getNumberOfReservedLowIds(), false );
    }

    private void createHeaderRecord( PageCursor cursor )
    {
        int offset = cursor.getOffset();
        storeHeaderFormat.writeHeader( cursor );
        cursor.setOffset( offset );
        readHeaderAndInitializeRecordFormat( cursor );
    }

    private void extractHeaderRecord( PagedFile pagedFile ) throws IOException
    {
        if ( getNumberOfReservedLowIds() > 0 )
        {
            try ( PageCursor pageCursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {

                if ( pageCursor.next() )
                {
                    do
                    {
                        pageCursor.setOffset( 0 );
                        readHeaderAndInitializeRecordFormat( pageCursor );
                    }
                    while ( pageCursor.shouldRetry() );
                    if ( pageCursor.checkAndClearBoundsFlag() )
                    {
                        throw new UnderlyingStorageException(
                                "Out of page bounds when reading header; page size too small: " +
                                pageCache.pageSize() + " bytes." );
                    }
                }
                else
                {
                    throw new StoreNotFoundException( "Fail to read header record of store file: " +
                                                      storageFileName );
                }
            }
        }
        else
        {
            readHeaderAndInitializeRecordFormat( null );
        }
        recordSize = determineRecordSize();
    }

    protected long pageIdForRecord( long id )
    {
        return RecordPageLocationCalculator.pageIdForRecord( id, storeFile.pageSize(), recordSize );
    }

    protected int offsetForId( long id )
    {
        return RecordPageLocationCalculator.offsetForId( id, storeFile.pageSize(), recordSize );
    }

    @Override
    public int getRecordsPerPage()
    {
        return storeFile.pageSize() / recordSize;
    }

    public byte[] getRawRecordData( long id ) throws IOException
    {
        byte[] data = new byte[recordSize];
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        try ( PageCursor cursor = storeFile.io( pageId, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            if ( cursor.next() )
            {
                do
                {
                    cursor.setOffset( offset );
                    cursor.getBytes( data );
                }
                while ( cursor.shouldRetry() );
                checkForDecodingErrors( cursor, id, CHECK );
            }
        }
        return data;
    }

    /**
     * This method is called when opening the store to extract header data and determine things like
     * record size of the specific record format for this store. Some formats rely on information
     * in the store header, that's why it happens at this stage.
     *
     * @param cursor {@link PageCursor} initialized at the start of the store header where header information
     * can be read if need be. This can be {@code null} if this store has no store header. The initialization
     * of the record format still happens in here.
     * @throws IOException if there were problems reading header information.
     */
    private void readHeaderAndInitializeRecordFormat( PageCursor cursor )
    {
        storeHeader = storeHeaderFormat.readHeader( cursor );
    }

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

    public boolean isInUse( long id )
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );

        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            boolean recordIsInUse = false;
            if ( cursor.next() )
            {
                do
                {
                    cursor.setOffset( offset );
                    recordIsInUse = isInUse( cursor );
                }
                while ( cursor.shouldRetry() );
                checkForDecodingErrors( cursor, id, NORMAL );
            }
            return recordIsInUse;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * DANGER: make sure to always close this cursor.
     */
    public PageCursor openPageCursorForReading( long id )
    {
        try
        {
            long pageId = pageIdForRecord( id );
            return storeFile.io( pageId, PF_SHARED_READ_LOCK );
        }
        catch ( IOException e )
        {
            // TODO: think about what we really should be doing with the exception handling here...
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Should rebuild the id generator from scratch.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    final void rebuildIdGenerator()
    {
        int blockSize = getRecordSize();
        if ( blockSize <= 0 )
        {
            throw new InvalidRecordException( "Illegal blockSize: " + blockSize );
        }

        log.info( "Rebuilding id generator for[" + getStorageFileName() + "] ..." );
        closeIdGenerator();
        createIdGenerator( getIdFileName() );
        openIdGenerator();

        long defraggedCount = 0;
        boolean fastRebuild = isOnlyFastIdGeneratorRebuildEnabled( configuration );

        try
        {
            long foundHighId = scanForHighId();
            setHighId( foundHighId );
            if ( !fastRebuild )
            {
                try ( PageCursor cursor = storeFile.io( 0, PF_SHARED_WRITE_LOCK | PF_READ_AHEAD ) )
                {
                    defraggedCount = rebuildIdGeneratorSlow( cursor, getRecordsPerPage(), blockSize, foundHighId );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to rebuild id generator " + getStorageFileName(), e );
        }

        log.info( "[" + getStorageFileName() + "] high id=" + getHighId() + " (defragged=" + defraggedCount + ")" );
        log.info( getStorageFileName() + " rebuild id generator, highId=" + getHighId() +
                  " defragged count=" + defraggedCount );

        if ( !fastRebuild )
        {
            closeIdGenerator();
            openIdGenerator();
        }
    }

    protected boolean isOnlyFastIdGeneratorRebuildEnabled( Config config )
    {
        return config.get( Configuration.rebuild_idgenerators_fast );
    }

    private long rebuildIdGeneratorSlow( PageCursor cursor, int recordsPerPage, int blockSize,
                                         long foundHighId ) throws IOException
    {
        if ( !cursor.isWriteLocked() )
        {
            throw new IllegalArgumentException(
                    "The store scanning id generator rebuild process requires a page cursor that is write-locked" );
        }
        long defragCount = 0;
        long[] freedBatch = new long[recordsPerPage]; // we process in batches of one page worth of records
        int startingId = getNumberOfReservedLowIds();
        int defragged;

        boolean done = false;
        while ( !done && cursor.next() )
        {
            long idPageOffset = cursor.getCurrentPageId() * recordsPerPage;

            defragged = 0;
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

                if ( !isInUse( cursor ) )
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
            checkIdScanCursorBounds( cursor );

            for ( int i = 0; i < defragged; i++ )
            {
                freeId( freedBatch[i] );
            }
            defragCount += defragged;
            startingId = 0;
        }
        return defragCount;
    }

    private void checkIdScanCursorBounds( PageCursor cursor )
    {
        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw new UnderlyingStorageException(
                    "Out of bounds access on page " + cursor.getCurrentPageId() + " detected while scanning the " +
                    storageFileName + " file for deleted records" );
        }
    }

    /**
     * Marks this store as "not ok".
     */
    void setStoreNotOk( RuntimeException cause )
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
    boolean getStoreOk()
    {
        return storeOk;
    }

    /**
     * Throws cause of not being OK if {@link #getStoreOk()} returns {@code false}.
     */
    void checkStoreOk()
    {
        if ( !storeOk )
        {
            throw causeOfStoreNotOk;
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
        assertIdGeneratorInitialized();
        return idGenerator.nextId();
    }

    private void assertIdGeneratorInitialized()
    {
        if ( idGenerator == null )
        {
            throw new IllegalStateException( "IdGenerator is not initialized" );
        }
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        assertIdGeneratorInitialized();
        return idGenerator.nextIdBatch( size );
    }

    /**
     * Frees an id for this store's {@link IdGenerator}.
     *
     * @param id The id to free
     */
    @Override
    public void freeId( long id )
    {
        IdGenerator generator = this.idGenerator;
        if ( generator != null )
        {
            generator.freeId( id );
        }
        // else we're deleting records as part of applying transactions during recovery, and that's fine
    }

    /**
     * Return the highest id in use. If this store is not OK yet, the high id is calculated from the highest
     * in use record on the store, using {@link #scanForHighId()}.
     *
     * @return The high id, i.e. highest id in use + 1.
     */
    @Override
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
        IdGenerator generator = this.idGenerator;
        if ( generator != null )
        {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized ( generator )
            {
                if ( highId > generator.getHighId() )
                {
                    generator.setHighId( highId );
                }
            }
        }
    }

    /**
     * If store is not ok a call to this method will rebuild the {@link
     * IdGenerator} used by this store and if successful mark it as OK.
     *
     * WARNING: this method must NOT be called if recovery is required, but hasn't performed.
     * To remove all negations from the above statement: Only call this method if store is in need of
     * recovery and recovery has been performed.
     */
    void makeStoreOk()
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
    @Override
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
    void openIdGenerator()
    {
        idGenerator = idGeneratorFactory.open( getIdFileName(), getIdType(), this::scanForHighId, recordFormat.getMaxId() );
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
        try ( PageCursor cursor = storeFile.io( 0, PF_SHARED_READ_LOCK ) )
        {
            int recordsPerPage = getRecordsPerPage();
            int recordSize = getRecordSize();
            long highestId = getNumberOfReservedLowIds();
            boolean found;
            /*
             * We do this in chunks of pages instead of one page at a time, the performance impact is significant.
             * We first pre-fetch a large chunk sequentially, which is then scanned backwards for used records.
             */
            final long chunkSizeInPages = 256; // 2MiB (8192 bytes/page * 256 pages/chunk)

            long chunkEndId = storeFile.getLastPageId();
            while ( chunkEndId >= 0 )
            {
                // Do pre-fetch of the chunk
                long chunkStartId = max( chunkEndId - chunkSizeInPages, 0 );
                preFetchChunk( cursor, chunkStartId, chunkEndId );

                // Scan pages backwards in the chunk
                for ( long currentId = chunkEndId; currentId >= chunkStartId && cursor.next( currentId ); currentId-- )
                {
                    do
                    {
                        found = false;
                        // Scan record backwards in the page
                        for ( int offset = recordsPerPage * recordSize - recordSize; offset >= 0; offset -= recordSize )
                        {
                            cursor.setOffset( offset );
                            if ( isInUse( cursor ) )
                            {
                                // We've found the highest id in use
                                highestId = (cursor.getCurrentPageId() * recordsPerPage) + offset / recordSize + 1;
                                found = true;
                                break;
                            }
                        }
                    }
                    while ( cursor.shouldRetry() );

                    checkIdScanCursorBounds( cursor );
                    if ( found )
                    {
                        return highestId;
                    }
                }
                chunkEndId = chunkStartId - 1;
            }

            return getNumberOfReservedLowIds();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to find high id by scanning backwards " + getStorageFileName(), e );
        }
    }

    /**
     * Do a pre-fetch of pages in sequential order on the range [{@code pageIdStart},{@code pageIdEnd}].
     *
     * @param cursor Cursor to pre-fetch on.
     * @param pageIdStart Page id to start pre-fetching from.
     * @param pageIdEnd Page id to end pre-fetching on, inclusive {@code pageIdEnd}.
     */
    private static void preFetchChunk( PageCursor cursor, long pageIdStart, long pageIdEnd ) throws IOException
    {
        for ( long currentPageId = pageIdStart; currentPageId <= pageIdEnd; currentPageId++ )
        {
            cursor.next( currentPageId );
        }
    }

    protected int determineRecordSize()
    {
        return recordFormat.getRecordSize( storeHeader );
    }

    @Override
    public final int getRecordSize()
    {
        return recordSize;
    }

    @Override
    public int getRecordDataSize()
    {
        return recordSize - recordFormat.getRecordHeaderSize();
    }

    private boolean isInUse( PageCursor cursor )
    {
        return recordFormat.isInUse( cursor );
    }

    protected boolean isRecordReserved( PageCursor cursor )
    {
        return false;
    }

    private void createIdGenerator( File fileName )
    {
        idGeneratorFactory.create( fileName, 0, false );
    }

    /** Closed the {@link IdGenerator} used by this store */
    void closeIdGenerator()
    {
        if ( idGenerator != null )
        {
            idGenerator.close();
        }
    }

    @Override
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
     * Checks if this store is closed and throws exception if it is.
     *
     * @throws IllegalStateException if the store is closed
     */
    void assertNotClosed()
    {
        if ( storeFile == null )
        {
            throw new IllegalStateException( this + " for file '" + storageFileName + "' is closed" );
        }
    }

    /**
     * Closes this store. This will cause all buffers and channels to be closed.
     * Requesting an operation from after this method has been invoked is
     * illegal and an exception will be thrown.
     * <p>
     * This method will start by invoking the {@link #closeStoreFile()} method
     * giving the implementing store way to do anything that it needs to do
     * before the pagedFile is closed.
     */
    @Override
    public void close()
    {
        try
        {
            closeStoreFile();
        }
        catch ( IOException | IllegalStateException e )
        {
            throw new UnderlyingStorageException( "Failed to close store file: " + getStorageFileName(), e );
        }
    }

    private void closeStoreFile() throws IOException
    {
        try
        {
            /*
             * Note: the closing ordering here is important!
             * It is the case since we wand to mark the id generator as closed cleanly ONLY IF
             * also the store file is cleanly shutdown.
             */
            if ( storeFile != null )
            {
                storeFile.close();
            }
            if ( idGenerator != null )
            {
                if ( contains( openOptions, DELETE_ON_CLOSE ) )
                {
                    idGenerator.delete();
                }
                else
                {
                    idGenerator.close();
                }
            }
        }
        finally
        {
            storeFile = null;
        }
    }

    /** @return The highest possible id in use, -1 if no id in use. */
    @Override
    public long getHighestPossibleIdInUse()
    {
        return idGenerator != null ? idGenerator.getHighestPossibleIdInUse() : scanForHighId() - 1;
    }

    /**
     * Sets the highest id in use. After this call highId will be this given id + 1.
     *
     * @param highId The highest id in use to set.
     */
    @Override
    public void setHighestPossibleIdInUse( long highId )
    {
        setHighId( highId + 1 );
    }

    /** @return The total number of ids in use. */
    public long getNumberOfIdsInUse()
    {
        assertIdGeneratorInitialized();
        return idGenerator.getNumberOfIdsInUse();
    }

    /**
     * @return the number of records at the beginning of the store file that are reserved for other things
     * than actual records. Stuff like permanent configuration data.
     */
    @Override
    public int getNumberOfReservedLowIds()
    {
        return storeHeaderFormat.numberOfReservedRecords();
    }

    public IdType getIdType()
    {
        return idType;
    }

    void logVersions( Logger logger )
    {
        logger.log( "  " + getTypeDescriptor() + " " + storeVersion );
    }

    void logIdUsage( Logger logger )
    {
        logger.log( String.format( "  %s: used=%s high=%s",
                getTypeDescriptor(), getNumberOfIdsInUse(), getHighestPossibleIdInUse() ) );
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #makeStoreOk()},
     * {@link #close()} (where that method could be deleted all together and do a visit in {@link #close()}),
     * {@link #logIdUsage(Logger)},
     * {@link #logVersions(Logger)}
     * For a good samaritan to pick up later.
     */
    void visitStore( Visitor<CommonAbstractStore<RECORD,HEADER>,RuntimeException> visitor )
    {
        visitor.visit( this );
    }

    /**
     * Called from the part of the code that starts the {@link MetaDataStore} and friends, together with any
     * existing transaction log, seeing that there are transactions to recover. Now, this shouldn't be
     * needed because the state of the id generator _should_ reflect this fact, but turns out that,
     * given HA and the nature of the .id files being like orphans to the rest of the store, we just
     * can't trust that to be true. If we happen to have id generators open during recovery we delegate
     * {@link #freeId(long)} calls to {@link IdGenerator#freeId(long)} and since the id generator is most likely
     * out of date w/ regards to high id, it may very well blow up.
     *
     * This also marks the store as not OK. A call to {@link #makeStoreOk()} is needed once recovery is complete.
     */
    final void deleteIdGenerator()
    {
        if ( idGenerator != null )
        {
            idGenerator.delete();
            idGenerator = null;
            setStoreNotOk( new IllegalStateException( "IdGenerator is not initialized" ) );
        }
    }

    @Override
    public long getNextRecordReference( RECORD record )
    {
        return recordFormat.getNextRecordReference( record );
    }

    @Override
    public RECORD newRecord()
    {
        return recordFormat.newRecord();
    }

    /**
     * Acquires a {@link PageCursor} from the {@link PagedFile store file} and reads the requested record
     * in the correct page and offset.
     *
     * @param id the record id.
     * @param record the record instance to load the data into.
     * @param mode how strict to be when loading, f.ex {@link RecordLoad#FORCE} will always read what's there
     * and load into the record, whereas {@link RecordLoad#NORMAL} will throw {@link InvalidRecordException}
     * if not in use.
     */
    @Override
    public RECORD getRecord( long id, RECORD record, RecordLoad mode )
    {
        try ( PageCursor cursor = storeFile.io( getNumberOfReservedLowIds(), PF_SHARED_READ_LOCK ) )
        {
            readIntoRecord( id, record, mode, cursor );
            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void getRecordByCursor( long id, RECORD record, RecordLoad mode, PageCursor cursor ) throws UnderlyingStorageException
    {
        try
        {
            readIntoRecord( id, record, mode, cursor );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }

    }

    void readIntoRecord( long id, RECORD record, RecordLoad mode, PageCursor cursor ) throws IOException
    {
        // Mark the record with this id regardless of whether or not we load the contents of it.
        // This is done in this method since there are multiple call sites and they all want the id
        // on that record, so it's to ensure it isn't forgotten.
        record.setId( id );
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        if ( cursor.next( pageId ) )
        {
            // There is a page in the store that covers this record, go read it
            do
            {
                prepareForReading( cursor, offset, record );
                recordFormat.read( record, cursor, mode, recordSize );
            }
            while ( cursor.shouldRetry() );
            checkForDecodingErrors( cursor, id, mode );
            verifyAfterReading( record, mode );
        }
        else
        {
            verifyAfterNotRead( record, mode );
        }
    }

    @Override
    public void updateRecord( RECORD record )
    {
        long id = record.getId();
        IdValidator.assertValidId( getIdType(), id, recordFormat.getMaxId() );

        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_WRITE_LOCK ) )
        {
            if ( cursor.next() )
            {
                cursor.setOffset( offset );
                recordFormat.write( record, cursor, recordSize );
                checkForDecodingErrors( cursor, id, NORMAL ); // We don't free ids if something weird goes wrong
                if ( !record.inUse() )
                {
                    freeId( id );
                }
                if ( (!record.inUse() || !record.requiresSecondaryUnit()) && record.hasSecondaryUnitId() )
                {
                    // If record was just now deleted, or if the record used a secondary unit, but not anymore
                    // then free the id of that secondary unit.
                    freeId( record.getSecondaryUnitId() );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void prepareForCommit( RECORD record )
    {
        prepareForCommit( record, this );
    }

    @Override
    public void prepareForCommit( RECORD record, IdSequence idSequence )
    {
        if ( record.inUse() )
        {
            recordFormat.prepare( record, recordSize, idSequence );
        }
    }

    @Override
    public <EXCEPTION extends Exception> void scanAllRecords( Visitor<RECORD,EXCEPTION> visitor ) throws EXCEPTION
    {
        try ( RecordCursor<RECORD> cursor = newRecordCursor( newRecord() ) )
        {
            long highId = getHighId();
            cursor.acquire( getNumberOfReservedLowIds(), CHECK );
            for ( long id = getNumberOfReservedLowIds(); id < highId; id++ )
            {
                if ( cursor.next( id ) )
                {
                    visitor.visit( cursor.get() );
                }
            }
        }
    }

    @Override
    public Collection<RECORD> getRecords( long firstId, RecordLoad mode )
    {
        try ( RecordCursor<RECORD> cursor = newRecordCursor( newRecord() ) )
        {
            cursor.acquire( firstId, mode );
            return cursor.getAll();
        }
    }

    @Override
    public RecordCursor<RECORD> newRecordCursor( final RECORD record )
    {
        return new StoreRecordCursor<>( record, this );
    }

    private void verifyAfterNotRead( RECORD record, RecordLoad mode )
    {
        record.clear();
        mode.verify( record );

    }

    final void checkForDecodingErrors( PageCursor cursor, long recordId, RecordLoad mode )
    {
        if ( mode.checkForOutOfBounds( cursor ) )
        {
            throwOutOfBoundsException( recordId );
        }
        mode.clearOrThrowCursorError( cursor );
    }

    private void throwOutOfBoundsException( long recordId )
    {
        RECORD record = newRecord();
        record.setId( recordId );
        long pageId = pageIdForRecord( recordId );
        int offset = offsetForId( recordId );
        throw new UnderlyingStorageException( buildOutOfBoundsExceptionMessage(
                record, pageId, offset, recordSize, storeFile.pageSize(), storageFileName.getAbsolutePath() ) );
    }

    static String buildOutOfBoundsExceptionMessage( AbstractBaseRecord record, long pageId, int offset, int recordSize,
            int pageSize, String filename )
    {
        return "Access to record " + record + " went out of bounds of the page. The record size is " +
               recordSize + " bytes, and the access was at offset " + offset + " bytes into page " +
               pageId + ", and the pages have a capacity of " + pageSize + " bytes. " +
               "The mapped store file in question is " + filename;
    }

    private void verifyAfterReading( RECORD record, RecordLoad mode )
    {
        if ( !mode.verify( record ) )
        {
            record.clear();
        }
    }

    private void prepareForReading( PageCursor cursor, int offset, RECORD record )
    {
        // Mark this record as unused. This to simplify implementations of readRecord.
        // readRecord can behave differently depending on RecordLoad argument and so it may be that
        // contents of a record may be loaded even if that record is unused, where the contents
        // can still be initialized data. Know that for many record stores, deleting a record means
        // just setting one byte or bit in that record.
        record.setInUse( false );
        cursor.setOffset( offset );
    }

    @Override
    public void ensureHeavy( RECORD record )
    {
        // Do nothing by default. Some record stores have this.
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    @Override
    public int getStoreHeaderInt()
    {
        return ((IntStoreHeader) storeHeader).value();
    }

    public abstract static class Configuration
    {
        static final Setting<Boolean> rebuild_idgenerators_fast = GraphDatabaseSettings.rebuild_idgenerators_fast;
    }
}
