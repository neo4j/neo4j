/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongPredicate;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdRange;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.util.concurrent.Runnables;

import static java.lang.Math.max;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.ANY_PAGE_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_EAGER_FLUSH;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
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
    protected final IdType idType;
    protected final IdGeneratorFactory idGeneratorFactory;
    protected final Log log;
    protected final String storeVersion;
    protected final RecordFormat<RECORD> recordFormat;
    final File storageFile;
    private final File idFile;
    private final String typeDescriptor;
    protected PagedFile pagedFile;
    protected int recordSize;
    private int filePageSize;
    private int recordsPerPage;
    private int recordsEndOffset;
    private IdGenerator idGenerator;
    private boolean storeOk = true;
    private RuntimeException causeOfStoreNotOk;

    private final StoreHeaderFormat<HEADER> storeHeaderFormat;
    private HEADER storeHeader;

    private final ImmutableSet<OpenOption> openOptions;

    /**
     * Opens and validates the store contained in <CODE>file</CODE>
     * loading any configuration defined in <CODE>config</CODE>. After
     * validation the <CODE>initStorage</CODE> method is called.
     * <p>
     * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>.
     * If a problem was found when opening the store the {@link #start(PageCursorTracer)}
     * must be invoked.
     * <p>
     * throws IOException if the unable to open the storage or if the
     * <CODE>initStorage</CODE> method fails
     *
     * @param idType The Id used to index into this store
     */
    public CommonAbstractStore(
            File file,
            File idFile,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            String typeDescriptor,
            RecordFormat<RECORD> recordFormat,
            StoreHeaderFormat<HEADER> storeHeaderFormat,
            String storeVersion,
            ImmutableSet<OpenOption> openOptions )
    {
        this.storageFile = file;
        this.idFile = idFile;
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

    protected void initialise( boolean createIfNotExists, PageCursorTracer cursorTracer )
    {
        try
        {
            boolean created = checkAndLoadStorage( createIfNotExists, cursorTracer );
            if ( !created )
            {
                openIdGenerator( cursorTracer );
            }
        }
        catch ( Exception e )
        {
            closeAndThrow( e );
        }
    }

    private void closeAndThrow( Exception e )
    {
        closeStoreFile();
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
     * @return {@code true} if the store was created as part of this call, otherwise {@code false} if it already existed.
     */
    private boolean checkAndLoadStorage( boolean createIfNotExists, PageCursorTracer cursorTracer )
    {
        try
        {
            determineRecordSize( storeHeaderFormat.generateHeader() );
            if ( getNumberOfReservedLowIds() > 0 )
            {
                // This store has a store-specific header so we have read it before we can be sure that we can map it with correct page size.
                // Try to open the store file (w/o creating if it doesn't exist), with page size for the configured header value.
                HEADER defaultHeader = storeHeaderFormat.generateHeader();
                pagedFile = pageCache.map( storageFile, filePageSize, openOptions.newWith( ANY_PAGE_SIZE ) );
                HEADER readHeader = readStoreHeaderAndDetermineRecordSize( pagedFile, cursorTracer );
                if ( !defaultHeader.equals( readHeader ) )
                {
                    // The header that we read was different from the default one so unmap
                    pagedFile.close();
                    pagedFile = null;
                }
            }

            if ( pagedFile == null )
            {
                // Map the file with the correct page size
                pagedFile = pageCache.map( storageFile, filePageSize, openOptions );
            }
        }
        catch ( NoSuchFileException | StoreNotFoundException e )
        {
            if ( pagedFile != null )
            {
                pagedFile.close();
                pagedFile = null;
            }

            if ( createIfNotExists )
            {
                try
                {
                    // Generate the header and determine correct page size
                    determineRecordSize( storeHeaderFormat.generateHeader() );

                    // Create the id generator, and also open it because some stores may need the id generator when initializing their store
                    boolean readOnly = configuration.get( GraphDatabaseSettings.read_only );
                    idGenerator = idGeneratorFactory.create( pageCache, idFile, idType, getNumberOfReservedLowIds(), false, recordFormat.getMaxId(),
                            readOnly, cursorTracer, openOptions );

                    // Map the file (w/ the CREATE flag) and initialize the header
                    pagedFile = pageCache.map( storageFile, filePageSize, openOptions.newWith( CREATE ) );
                    initialiseNewStoreFile( cursorTracer );
                    return true; // <-- successfully created and initialized
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
            throw new StoreNotFoundException( "Store file not found: " + storageFile, e );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open store file: " + storageFile, e );
        }
        return false;
    }

    protected void initialiseNewStoreFile( PageCursorTracer cursorTracer ) throws IOException
    {
        if ( getNumberOfReservedLowIds() > 0 )
        {
            try ( PageCursor pageCursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK | PF_EAGER_FLUSH, cursorTracer ) )
            {
                if ( pageCursor.next() )
                {
                    pageCursor.setOffset( 0 );
                    storeHeaderFormat.writeHeader( pageCursor );
                    if ( pageCursor.checkAndClearBoundsFlag() )
                    {
                        throw new UnderlyingStorageException(
                                "Out of page bounds when writing header; page size too small: " + pageCache.pageSize() + " bytes." );
                    }
                }
            }
            pagedFile.flushAndForce();
        }

        // Determine record size right after writing the header since some stores
        // use it when initializing their stores to write some records.
        recordSize = determineRecordSize();
    }

    private HEADER readStoreHeaderAndDetermineRecordSize( PagedFile pagedFile, PageCursorTracer cursorTracer ) throws IOException
    {
        try ( PageCursor pageCursor = pagedFile.io( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            HEADER readHeader;
            if ( pageCursor.next() )
            {
                do
                {
                    pageCursor.setOffset( 0 );
                    readHeader = readStoreHeaderAndDetermineRecordSize( pageCursor );
                }
                while ( pageCursor.shouldRetry() );
                if ( pageCursor.checkAndClearBoundsFlag() )
                {
                    throw new UnderlyingStorageException(
                            "Out of page bounds when reading header; page size too small: " +
                            pageCache.pageSize() + " bytes." );
                }
                return readHeader;
            }
            else
            {
                throw new StoreNotFoundException( "Fail to read header record of store file: " + storageFile );
            }
        }
    }

    protected long pageIdForRecord( long id )
    {
        return RecordPageLocationCalculator.pageIdForRecord( id, recordsPerPage );
    }

    protected int offsetForId( long id )
    {
        return RecordPageLocationCalculator.offsetForId( id, recordSize, recordsPerPage );
    }

    @Override
    public int getRecordsPerPage()
    {
        return recordsPerPage;
    }

    public long getLastPageId() throws IOException
    {
        return pagedFile.getLastPageId();
    }

    /**
     * Read raw record data. Should <strong>ONLY</strong> be used in tests or tools.
     */
    public byte[] getRawRecordData( long id, PageCursorTracer cursorTracer ) throws IOException
    {
        byte[] data = new byte[recordSize];
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        try ( PageCursor cursor = pagedFile.io( pageId, PagedFile.PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            if ( cursor.next() )
            {
                cursor.setOffset( offset );
                cursor.mark();
                do
                {
                    cursor.setOffsetToMark();
                    cursor.getBytes( data );
                }
                while ( cursor.shouldRetry() );
                checkForDecodingErrors( cursor, id, FORCE ); // Clear errors from the cursor.
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
     */
    private HEADER readStoreHeaderAndDetermineRecordSize( PageCursor cursor )
    {
        HEADER header = storeHeaderFormat.readHeader( cursor );
        determineRecordSize( header );
        return header;
    }

    private void determineRecordSize( HEADER header )
    {
        storeHeader = header;
        recordSize = determineRecordSize();
        filePageSize = recordFormat.getPageSize( pageCache.pageSize(), recordSize );
        recordsPerPage = filePageSize / recordSize;
        recordsEndOffset = recordsPerPage * recordSize; // Truncated file page size to whole multiples of record size.
    }

    public boolean isInUse( long id, PageCursorTracer cursorTracer )
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );

        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            boolean recordIsInUse = false;
            if ( cursor.next() )
            {
                cursor.setOffset( offset );
                cursor.mark();
                do
                {
                    cursor.setOffsetToMark();
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
     *
     * Opens a {@link PageCursor} to this store, mainly for use in {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}.
     * The opened cursor will make use of the {@link PagedFile#PF_READ_AHEAD} flag for optimal scanning performance.
     */
    @Override
    public PageCursor openPageCursorForReadingWithPrefetching( long id, PageCursorTracer cursorTracer )
    {
        return openPageCursorForReading( 0, PF_READ_AHEAD, cursorTracer );
    }

    /**
     * DANGER: make sure to always close this cursor.
     */
    @Override
    public PageCursor openPageCursorForReading( long id, PageCursorTracer cursorTracer )
    {
        return openPageCursorForReading( id, 0, cursorTracer );
    }

    private PageCursor openPageCursorForReading( long id, int additionalCursorFlags, PageCursorTracer cursorTracer )
    {
        try
        {
            long pageId = pageIdForRecord( id );
            return pagedFile.io( pageId, PF_SHARED_READ_LOCK | additionalCursorFlags, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void checkIdScanCursorBounds( PageCursor cursor )
    {
        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw new UnderlyingStorageException(
                    "Out of bounds access on page " + cursor.getCurrentPageId() + " detected while scanning the " + storageFile + " file for deleted records" );
        }
    }

    /**
     * Marks this store as "not ok".
     */
    void setStoreNotOk( RuntimeException cause )
    {
        storeOk = false;
        causeOfStoreNotOk = cause;
    }

    /**
     * Throws cause of not being OK, i.e. if {@link #setStoreNotOk(RuntimeException)} have been called.
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
    public long nextId( PageCursorTracer cursorTracer )
    {
        assertIdGeneratorInitialized();
        return idGenerator.nextId( cursorTracer );
    }

    private void assertIdGeneratorInitialized()
    {
        if ( idGenerator == null )
        {
            throw new IllegalStateException( "IdGenerator is not initialized" );
        }
    }

    @Override
    public IdRange nextIdBatch( int size, PageCursorTracer cursorTracer )
    {
        assertIdGeneratorInitialized();
        return idGenerator.nextIdBatch( size, cursorTracer );
    }

    /**
     * Return the highest id in use. If this store is not OK yet, the high id is calculated from the highest
     * in use record on the store, using {@link #scanForHighId(PageCursorTracer)}.
     *
     * @return The high id, i.e. highest id in use + 1.
     */
    @Override
    public long getHighId()
    {
        return idGenerator.getHighId();
    }

    /**
     * Sets the high id, i.e. highest id in use + 1 (use this when rebuilding id generator).
     *
     * @param highId The high id to set.
     */
    public void setHighId( long highId )
    {
        idGenerator.setHighId( highId );
    }

    /**
     * Sets the store state to started, which is a state which either means that:
     * <ul>
     *     <li>store was opened on a previous clean shutdown where no recovery was required</li>
     *     <li>store was opened on a previous non-clean shutdown and recovery has been performed</li>
     * </ul>
     * So when this method is called the store is in a good state and from this point the database enters normal operations mode.
     */
    void start( PageCursorTracer cursorTracer ) throws IOException
    {
        if ( !storeOk )
        {
            storeOk = true;
            causeOfStoreNotOk = null;
        }
        idGenerator.start( freeIds( cursorTracer ), cursorTracer );
    }

    private FreeIds freeIds( PageCursorTracer cursorTracer )
    {
        return visitor ->
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK | PF_READ_AHEAD, cursorTracer ) )
            {
                int numberOfReservedLowIds = getNumberOfReservedLowIds();
                int startingId = numberOfReservedLowIds;
                int recordsPerPage = getRecordsPerPage();
                int blockSize = getRecordSize();
                long foundHighId = scanForHighId( cursorTracer );
                long[] foundIds = new long[recordsPerPage];
                int foundIdsCursor;

                boolean done = false;
                while ( !done && cursor.next() )
                {
                    do
                    {
                        foundIdsCursor = 0;
                        long idPageOffset = cursor.getCurrentPageId() * recordsPerPage;
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
                                foundIds[foundIdsCursor++] = recordId;
                            }
                        }
                    }
                    while ( cursor.shouldRetry() );
                    startingId = 0;
                    checkIdScanCursorBounds( cursor );

                    for ( int i = 0; i < foundIdsCursor; i++ )
                    {
                        visitor.accept( foundIds[i] );
                    }
                }
                return Long.max( numberOfReservedLowIds, foundHighId ) - 1;
            }
        };
    }

    /**
     * Returns the name of this store.
     *
     * @return The name of this store
     */
    @Override
    public File getStorageFile()
    {
        return storageFile;
    }

    /**
     * Opens the {@link IdGenerator} used by this store.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    private void openIdGenerator( PageCursorTracer cursorTracer )
    {
        boolean readOnly = configuration.get( GraphDatabaseSettings.read_only );
        idGenerator = idGeneratorFactory.open( pageCache, idFile, getIdType(), () -> scanForHighId( cursorTracer ), recordFormat.getMaxId(), readOnly,
                cursorTracer, openOptions );
    }

    /**
     * Starts from the end of the file and scans backwards to find the highest in use record.
     * Can be used even if {@link #start(PageCursorTracer)} hasn't been called. Basically this method should be used
     * over {@link #getHighestPossibleIdInUse(PageCursorTracer)} and {@link #getHighId()} in cases where a store has been opened
     * but is in a scenario where recovery isn't possible, like some tooling or migration.
     *
     * @return the id of the highest in use record + 1, i.e. highId.
     */
    protected long scanForHighId( PageCursorTracer cursorTracer )
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
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

            long chunkEndId = pagedFile.getLastPageId();
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
                        long basePageId = cursor.getCurrentPageId() * recordsPerPage;
                        for ( int record = 0; record < recordsPerPage; record++ )
                        {
                            cursor.setOffset( record * recordSize );
                            if ( isInUse( cursor ) )
                            {
                                // We've found the highest id in use
                                highestId = basePageId + record + 1;
                                found = true;
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
            throw new UnderlyingStorageException( "Unable to find high id by scanning backwards " + getStorageFile(), e );
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

    public long getStoreSize() throws IOException
    {
        return pagedFile.fileSize();
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

    @Override
    public void flush( PageCursorTracer cursorTracer )
    {
        try
        {
            pagedFile.flushAndForce();
            idGenerator.checkpoint( IOLimiter.UNLIMITED, cursorTracer );
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
        catch ( IllegalStateException e )
        {
            throw new UnderlyingStorageException( "Failed to close store file: " + getStorageFile(), e );
        }
    }

    private void closeStoreFile()
    {
        Runnables.runAll( "Failure closing store and/or id generator",
                () ->
                {
                    if ( pagedFile != null )
                    {
                        pagedFile.close();
                        pagedFile = null;
                    }
                },
                () ->
                {
                    if ( idGenerator != null )
                    {
                        idGenerator.close();
                        idGenerator = null;
                    }
                } );
    }

    /** @return The highest possible id in use, -1 if no id in use. */
    @Override
    public long getHighestPossibleIdInUse( PageCursorTracer cursorTracer )
    {
        return idGenerator != null ? idGenerator.getHighestPossibleIdInUse() : scanForHighId( cursorTracer ) - 1;
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
        logger.log( getTypeDescriptor() + ' ' + storeVersion );
    }

    void logIdUsage( Logger logger, PageCursorTracer cursorTracer )
    {
        logger.log( format( "%s: used=%s high=%s", getTypeDescriptor(), getNumberOfIdsInUse(), getHighestPossibleIdInUse( cursorTracer ) ) );
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
    public RECORD getRecord( long id, RECORD record, RecordLoad mode, PageCursorTracer cursorTracer )
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
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

    private void readIntoRecord( long id, RECORD record, RecordLoad mode, PageCursor cursor ) throws IOException
    {
        // Mark the record with this id regardless of whether or not we load the contents of it.
        // This is done in this method since there are multiple call sites and they all want the id
        // on that record, so it's to ensure it isn't forgotten.
        record.setId( id );
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        if ( cursor.next( pageId ) )
        {
            cursor.setOffset( offset );
            readRecordFromPage( id, record, mode, cursor );
        }
        else
        {
            verifyAfterNotRead( record, mode );
        }
    }

    @Override
    public void nextRecordByCursor( RECORD record, RecordLoad mode, PageCursor cursor ) throws UnderlyingStorageException
    {
        if ( cursor.getCurrentPageId() < -1 )
        {
            throw new IllegalArgumentException( "Pages are assumed to be positive or -1 if not initialized" );
        }

        try
        {
            long id = record.getId() + 1;
            record.setId( id );
            long pageId = cursor.getCurrentPageId();
            if ( (cursor.getOffset() >= recordsEndOffset) || (pageId < 0) )
            {
                if ( !cursor.next() )
                {
                    verifyAfterNotRead( record, mode );
                    return;
                }
            }
            readRecordFromPage( id, record, mode, cursor );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void readRecordFromPage( long id, RECORD record, RecordLoad mode, PageCursor cursor ) throws IOException
    {
        cursor.mark();
        do
        {
            prepareForReading( cursor, record );
            recordFormat.read( record, cursor, mode, recordSize, recordsPerPage );
        }
        while ( cursor.shouldRetry() );
        checkForDecodingErrors( cursor, id, mode );
        verifyAfterReading( record, mode );
    }

    @Override
    public void updateRecord( RECORD record, IdUpdateListener idUpdateListener, PageCursorTracer cursorTracer )
    {
        long id = record.getId();
        IdValidator.assertValidId( getIdType(), id, recordFormat.getMaxId() );

        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK, cursorTracer ) )
        {
            if ( cursor.next() )
            {
                cursor.setOffset( offset );
                recordFormat.write( record, cursor, recordSize, recordsPerPage );
                checkForDecodingErrors( cursor, id, NORMAL ); // We don't free ids if something weird goes wrong
                if ( !record.inUse() )
                {
                    idUpdateListener.markIdAsUnused( idType, idGenerator, id, cursorTracer );
                }
                else if ( record.isCreated() )
                {
                    idUpdateListener.markIdAsUsed( idType, idGenerator, id, cursorTracer );
                }

                if ( (!record.inUse() || !record.requiresSecondaryUnit()) && record.hasSecondaryUnitId() )
                {
                    // If record was just now deleted, or if the record used a secondary unit, but not anymore
                    // then free the id of that secondary unit.
                    idUpdateListener.markIdAsUnused( idType, idGenerator, record.getSecondaryUnitId(), cursorTracer );
                }
                if ( record.inUse() && record.isSecondaryUnitCreated() )
                {
                    // Triggers on:
                    // - (a) record got created right now and has a secondary unit, or
                    // - (b) it already existed and just now grew into a secondary unit then mark the secondary unit as used
                    idUpdateListener.markIdAsUsed( idType, idGenerator, record.getSecondaryUnitId(), cursorTracer );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void prepareForCommit( RECORD record, PageCursorTracer cursorTracer )
    {
        prepareForCommit( record, this, cursorTracer );
    }

    @Override
    public void prepareForCommit( RECORD record, IdSequence idSequence, PageCursorTracer cursorTracer )
    {
        if ( record.inUse() )
        {
            recordFormat.prepare( record, recordSize, idSequence, cursorTracer );
        }
    }

    @Override
    public <EXCEPTION extends Exception> void scanAllRecords( Visitor<RECORD,EXCEPTION> visitor, PageCursorTracer cursorTracer ) throws EXCEPTION
    {
        try ( PageCursor cursor = openPageCursorForReading( 0, cursorTracer ) )
        {
            RECORD record = newRecord();
            long highId = getHighId();
            for ( long id = getNumberOfReservedLowIds(); id < highId; id++ )
            {
                getRecordByCursor( id, record, CHECK, cursor );
                if ( record.inUse() )
                {
                    visitor.visit( record );
                }
            }
        }
    }

    @Override
    public List<RECORD> getRecords( long firstId, RecordLoad mode, boolean guardForCycles, PageCursorTracer cursorTracer )
    {
        if ( Record.NULL_REFERENCE.is( firstId ) )
        {
            return Collections.emptyList();
        }
        LongPredicate cycleGuard = guardForCycles ? createRecordCycleGuard() : Predicates.ALWAYS_FALSE_LONG;

        List<RECORD> records = new ArrayList<>();
        long id = firstId;
        try ( PageCursor cursor = openPageCursorForReading( firstId, cursorTracer ) )
        {
            RECORD record;
            do
            {
                record = newRecord();
                if ( cycleGuard.test( id ) )
                {
                    throw newCycleDetectedException( firstId, id, record );
                }
                getRecordByCursor( id, record, mode, cursor );
                // Even unused records gets added and returned
                records.add( record );
                id = getNextRecordReference( record );
            }
            while ( !Record.NULL_REFERENCE.is( id ) );
        }
        return records;
    }

    private LongPredicate createRecordCycleGuard()
    {
        MutableLongSet observedSet = LongSets.mutable.empty();
        return id -> !observedSet.add( id );
    }

    private RecordChainCycleDetectedException newCycleDetectedException( long firstId, long conflictingId, RECORD record )
    {
        return new RecordChainCycleDetectedException( "Cycle detected in " + record.getClass().getSimpleName() + " chain starting at id " +
                firstId + ", and finding id " + conflictingId + " twice in the chain." );
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
                record, pageId, offset, recordSize, pagedFile.pageSize(), storageFile.getAbsolutePath() ) );
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

    private void prepareForReading( PageCursor cursor, RECORD record )
    {
        // Mark this record as unused. This to simplify implementations of readRecord.
        // readRecord can behave differently depending on RecordLoad argument and so it may be that
        // contents of a record may be loaded even if that record is unused, where the contents
        // can still be initialized data. Know that for many record stores, deleting a record means
        // just setting one byte or bit in that record.
        record.setInUse( false );
        cursor.setOffsetToMark();
    }

    public IdGenerator getIdGenerator()
    {
        return idGenerator;
    }

    @Override
    public void ensureHeavy( RECORD record, PageCursorTracer cursorTracer )
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
}
