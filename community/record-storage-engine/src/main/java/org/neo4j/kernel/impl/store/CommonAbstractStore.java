/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.ANY_PAGE_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_EAGER_FLUSH;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_CHAIN_FOLLOW;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.LENIENT_CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.util.concurrent.Runnables;

/**
 * Contains common implementation of {@link RecordStore}.
 */
public abstract class CommonAbstractStore<RECORD extends AbstractBaseRecord, HEADER extends StoreHeader>
        implements RecordStore<RECORD>, AutoCloseable {
    protected final Config configuration;
    protected final PageCache pageCache;
    protected final PageCacheTracer pageCacheTracer;
    protected final IdType idType;
    protected final IdGeneratorFactory idGeneratorFactory;
    protected final InternalLog log;
    protected final RecordFormat<RECORD> recordFormat;
    private final FileSystemAbstraction fileSystem;
    final Path storageFile;
    private final Path idFile;
    private final String typeDescriptor;
    protected final boolean readOnly;
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

    private final String databaseName;
    private final ImmutableSet<OpenOption> openOptions;

    /**
     * Opens and validates the store contained in <CODE>file</CODE>
     * loading any configuration defined in <CODE>config</CODE>. After
     * validation the <CODE>initStorage</CODE> method is called.
     * <p>
     * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>.
     * If a problem was found when opening the store the {@link #start(CursorContext)}
     * must be invoked.
     * <p>
     * throws IOException if the unable to open the storage or if the
     * <CODE>initStorage</CODE> method fails
     *
     * @param idType The Id used to index into this store
     */
    public CommonAbstractStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            String typeDescriptor,
            RecordFormat<RECORD> recordFormat,
            StoreHeaderFormat<HEADER> storeHeaderFormat,
            boolean readOnly,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        this.fileSystem = fileSystem;
        this.storageFile = path;
        this.idFile = idFile;
        this.configuration = configuration;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.idType = idType;
        this.pageCacheTracer = pageCacheTracer;
        this.typeDescriptor = typeDescriptor;
        this.recordFormat = recordFormat;
        this.storeHeaderFormat = storeHeaderFormat;
        this.databaseName = databaseName;
        this.openOptions = openOptions;
        this.readOnly = readOnly;
        this.log = logProvider.getLog(getClass());
    }

    protected void initialise(CursorContextFactory contextFactory) {
        try {
            boolean created = checkAndLoadStorage(contextFactory);
            if (!created) {
                openIdGenerator(contextFactory);
            }
        } catch (Exception e) {
            closeAndThrow(e);
        }
    }

    private void closeAndThrow(Exception e) {
        closeStoreFile();
        throwIfUnchecked(e);
        throw new RuntimeException(e);
    }

    /**
     * Returns the type and version that identifies this store.
     *
     * @return This store's implementation type and version identifier
     */
    public String getTypeDescriptor() {
        return typeDescriptor;
    }

    /**
     * This method is called by constructors. Checks the header record and loads the store.
     * <p>
     * Note: This method will map the file with the page cache. The store file must not
     * be accessed directly until it has been unmapped - the store file must only be
     * accessed through the page cache.
     * @return {@code true} if the store was created as part of this call, otherwise {@code false} if it already existed.
     */
    private boolean checkAndLoadStorage(CursorContextFactory contextFactory) {
        try (var cursorContext = contextFactory.create("checkAndLoadStorage")) {
            try {
                if (!readOnly && !fileSystem.fileExists(storageFile)) {
                    if (createNewStoreFile(contextFactory, cursorContext)) {
                        // store file was not there, and we just created it.
                        return true;
                    }
                } else if (openExistentStore(contextFactory, cursorContext)) {
                    return true; // <-- successfully created and initialized
                }
            } catch (IOException e) {
                throw new UnderlyingStorageException("Unable to open store file: " + storageFile, e);
            }
            return false;
        }
    }

    private boolean openExistentStore(CursorContextFactory contextFactory, CursorContext cursorContext)
            throws IOException {
        try {
            determineRecordSize(storeHeaderFormat.generateHeader());
            if (getNumberOfReservedLowIds() > 0) {
                // This store has a store-specific header so we have read it before we can be sure that we
                // can
                // map
                // it with correct page size.
                // Try to open the store file (w/o creating if it doesn't exist), with page size for the
                // configured
                // header value.
                HEADER defaultHeader = storeHeaderFormat.generateHeader();
                pagedFile = pageCache.map(storageFile, filePageSize, databaseName, openOptions.newWith(ANY_PAGE_SIZE));
                HEADER readHeader = readStoreHeaderAndDetermineRecordSize(pagedFile, cursorContext);
                if (!defaultHeader.equals(readHeader)) {
                    // The header that we read was different from the default one so unmap
                    pagedFile.close();
                    pagedFile = null;
                }
            }

            if (pagedFile == null) {
                // Map the file with the correct page size
                pagedFile = pageCache.map(storageFile, filePageSize, databaseName, openOptions);
            }
            determineRecordsPerPage();
        } catch (NoSuchFileException | StoreNotFoundException e) {
            if (pagedFile != null) {
                pagedFile.close();
                pagedFile = null;
            }

            // we can be here if existent store file was there, but it was corrupted in a way.
            if (!readOnly) {
                try {
                    if (createNewStoreFile(contextFactory, cursorContext)) {
                        return true;
                    }
                } catch (IOException e1) {
                    e.addSuppressed(e1);
                }
            }
            if (e instanceof StoreNotFoundException) {
                throw (StoreNotFoundException) e;
            }
            throw new StoreNotFoundException("Store file not found: " + storageFile, e);
        }
        return false;
    }

    private boolean createNewStoreFile(CursorContextFactory contextFactory, CursorContext cursorContext)
            throws IOException {
        // Generate the header and determine correct page size
        determineRecordSize(storeHeaderFormat.generateHeader());

        // Create the id generator, and also open it because some stores may need the id generator when
        // initializing their store
        idGenerator = idGeneratorFactory.create(
                pageCache,
                idFile,
                idType,
                getNumberOfReservedLowIds(),
                false,
                recordFormat.getMaxId(),
                readOnly,
                configuration,
                contextFactory,
                openOptions,
                SINGLE_IDS);

        // Map the file (w/ the CREATE flag) and initialize the header
        pagedFile = pageCache.map(storageFile, filePageSize, databaseName, openOptions.newWith(CREATE));
        try (FileFlushEvent flushEvent = pageCacheTracer.beginFileFlush()) {
            initialiseNewStoreFile(flushEvent, cursorContext);
        }
        return true;
    }

    protected void initialiseNewStoreFile(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        if (getNumberOfReservedLowIds() > 0) {
            try (PageCursor pageCursor = pagedFile.io(0, PF_SHARED_WRITE_LOCK | PF_EAGER_FLUSH, cursorContext)) {
                if (pageCursor.next()) {
                    storeHeaderFormat.writeHeader(pageCursor);
                    if (pageCursor.checkAndClearBoundsFlag()) {
                        throw new UnderlyingStorageException(
                                "Out of page bounds when writing header; page size too small: " + pageCache.pageSize()
                                        + " bytes.");
                    }
                }
            }
            pagedFile.flushAndForce(flushEvent);
        }

        // Determine record size right after writing the header since some stores
        // use it when initializing their stores to write some records.
        recordSize = determineRecordSize();
        determineRecordsPerPage();
    }

    private HEADER readStoreHeaderAndDetermineRecordSize(PagedFile pagedFile, CursorContext cursorContext)
            throws IOException {
        try (PageCursor pageCursor = pagedFile.io(0, PF_SHARED_READ_LOCK, cursorContext)) {
            HEADER readHeader;
            if (pageCursor.next()) {
                do {
                    pageCursor.setOffset(0);
                    readHeader = readStoreHeaderAndDetermineRecordSize(pageCursor);
                } while (pageCursor.shouldRetry());
                if (pageCursor.checkAndClearBoundsFlag()) {
                    throw new UnderlyingStorageException("Out of page bounds when reading header; page size too small: "
                            + pageCache.pageSize() + " bytes.");
                }
                return readHeader;
            } else {
                throw new StoreNotFoundException("Fail to read header record of store file: " + storageFile);
            }
        }
    }

    protected long pageIdForRecord(long id) {
        return RecordPageLocationCalculator.pageIdForRecord(id, recordsPerPage);
    }

    protected int offsetForId(long id) {
        return RecordPageLocationCalculator.offsetForId(id, recordSize, recordsPerPage);
    }

    @Override
    public int getRecordsPerPage() {
        return recordsPerPage;
    }

    public long getLastPageId() throws IOException {
        return pagedFile.getLastPageId();
    }

    /**
     * Read raw record data. Should <strong>ONLY</strong> be used in tests or tools.
     */
    public byte[] getRawRecordData(long id, PageCursor cursor) throws IOException {
        byte[] data = new byte[recordSize];
        long pageId = pageIdForRecord(id);
        int offset = offsetForId(id);
        if (cursor.next(pageId)) {
            cursor.setOffset(offset);
            cursor.mark();
            do {
                cursor.setOffsetToMark();
                cursor.getBytes(data);
            } while (cursor.shouldRetry());
            checkForDecodingErrors(cursor, id, FORCE); // Clear errors from the cursor.
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
    private HEADER readStoreHeaderAndDetermineRecordSize(PageCursor cursor) {
        HEADER header = storeHeaderFormat.readHeader(cursor);
        determineRecordSize(header);
        return header;
    }

    private void determineRecordSize(HEADER header) {
        storeHeader = header;
        recordSize = determineRecordSize();
        filePageSize = recordFormat.getFilePageSize(pageCache.pageSize(), recordSize);
    }

    /**
     * Determines number of records per page and end offset of the records.
     * Should be called after page file is mapped, as we need to know actual number of reserved bytes.
     */
    private void determineRecordsPerPage() {
        // We have aligned and non-aligned formats. For aligned formats file page size is the same as for page cache and
        // all is fine,
        // and it should be possible to use payload size easily but another second set of formats are non-aligned.
        // They have custom file page size that is used to map those files that are record size-dependent and the page
        // cache itself
        // has no idea about those dances. As result we can't use the payload to begin with for file page size there
        // since its a page size that should be used to map files and those are with reserved bytes
        recordsPerPage = (filePageSize - pagedFile.pageReservedBytes()) / recordSize;
        recordsEndOffset = recordsPerPage * recordSize; // Truncated file page size to whole multiples of record size.
    }

    public boolean isInUse(long id, PageCursor cursor) {
        long pageId = pageIdForRecord(id);
        int offset = offsetForId(id);
        try {
            boolean recordIsInUse = false;
            if (cursor.next(pageId)) {
                cursor.setOffset(offset);
                cursor.mark();
                do {
                    cursor.setOffsetToMark();
                    recordIsInUse = isInUse(cursor);
                } while (cursor.shouldRetry());
                checkForDecodingErrors(cursor, id, NORMAL);
            }
            return recordIsInUse;
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    /**
     * Opens a new {@link PageCursor} to this store, mainly for use in
     * {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor, MemoryTracker)}.
     * The opened cursor will make use of the {@link PagedFile#PF_READ_AHEAD} flag for optimal scanning performance.
     */
    @Override
    public PageCursor openPageCursorForReadingWithPrefetching(long id, CursorContext cursorContext) {
        return openPageCursorForReading(0, PF_READ_AHEAD, cursorContext);
    }

    /**
     * Opens a new {@link PageCursor} to this store.
     * The opened cursor will make not have any additional flags set.
     */
    @Override
    public PageCursor openPageCursorForReading(long id, CursorContext cursorContext) {
        return openPageCursorForReading(id, 0, cursorContext);
    }

    /**
     * Opens a new {@link PageCursor} to this store.
     * The opened cursor will make use of the {@link PagedFile#PF_NO_CHAIN_FOLLOW} flag set and will not follow chain links regardless of the version context.
     */
    @Override
    public PageCursor openPageCursorForReadingHeadOnly(long id, CursorContext cursorContext) {
        return openPageCursorForReading(id, PF_NO_CHAIN_FOLLOW, cursorContext);
    }

    private PageCursor openPageCursorForReading(long id, int additionalCursorFlags, CursorContext cursorContext) {
        try {
            long pageId = pageIdForRecord(id);
            return pagedFile.io(pageId, PF_SHARED_READ_LOCK | additionalCursorFlags, cursorContext);
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    @Override
    public PageCursor openPageCursorForWriting(long id, CursorContext cursorContext) {
        try {
            long pageId = pageIdForRecord(id);
            return pagedFile.io(pageId, PF_SHARED_WRITE_LOCK, cursorContext);
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    private void checkIdScanCursorBounds(PageCursor cursor) {
        if (cursor.checkAndClearBoundsFlag()) {
            throw new UnderlyingStorageException("Out of bounds access on page " + cursor.getCurrentPageId()
                    + " detected while scanning the " + storageFile + " file for deleted records");
        }
    }

    /**
     * Marks this store as "not ok".
     */
    void setStoreNotOk(RuntimeException cause) {
        storeOk = false;
        causeOfStoreNotOk = cause;
    }

    /**
     * Throws cause of not being OK, i.e. if {@link #setStoreNotOk(RuntimeException)} have been called.
     */
    void checkStoreOk() {
        if (!storeOk) {
            throw causeOfStoreNotOk;
        }
    }

    /**
     * Sets the high id, i.e. highest id in use + 1 (use this when rebuilding id generator).
     *
     * @param highId The high id to set.
     */
    public void setHighId(long highId) {
        idGenerator.setHighId(highId);
    }

    /**
     * @return {@code true} if this store has no records in it, i.e. is empty. Otherwise {@code false}.
     * This is different than checking if {@link IdGenerator#getHighId()} is larger than 0, since some stores may have
     * records in the beginning that are reserved, see {@link #getNumberOfReservedLowIds()}.
     */
    public boolean isEmpty() {
        return getIdGenerator().getHighId() == getNumberOfReservedLowIds();
    }

    /**
     * Sets the store state to started, which is a state which either means that:
     * <ul>
     *     <li>store was opened on a previous clean shutdown where no recovery was required</li>
     *     <li>store was opened on a previous non-clean shutdown and recovery has been performed</li>
     * </ul>
     * So when this method is called the store is in a good state and from this point the database enters normal operations mode.
     */
    void start(CursorContext cursorContext) throws IOException {
        if (!storeOk) {
            storeOk = true;
            causeOfStoreNotOk = null;
        }
        idGenerator.start(freeIds(cursorContext), cursorContext);
    }

    private FreeIds freeIds(CursorContext cursorContext) {
        return visitor -> {
            try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK | PF_READ_AHEAD, cursorContext)) {
                int numberOfReservedLowIds = getNumberOfReservedLowIds();
                int startingId = numberOfReservedLowIds;
                int recordsPerPage = getRecordsPerPage();
                int blockSize = getRecordSize();
                long foundHighId = scanForHighId(cursorContext);
                long[] foundIds = new long[recordsPerPage];
                int foundIdsCursor;

                boolean done = false;
                while (!done && cursor.next()) {
                    do {
                        foundIdsCursor = 0;
                        long idPageOffset = cursor.getCurrentPageId() * recordsPerPage;
                        for (int i = startingId; i < recordsPerPage; i++) {
                            int offset = i * blockSize;
                            cursor.setOffset(offset);
                            long recordId = idPageOffset + i;
                            if (recordId
                                    >= foundHighId) { // We don't have to go further than the high id we found earlier
                                done = true;
                                break;
                            }

                            if (!isInUse(cursor)) {
                                foundIds[foundIdsCursor++] = recordId;
                            }
                        }
                    } while (cursor.shouldRetry());
                    startingId = 0;
                    checkIdScanCursorBounds(cursor);

                    for (int i = 0; i < foundIdsCursor; i++) {
                        visitor.accept(foundIds[i]);
                    }
                }
                return Long.max(numberOfReservedLowIds, foundHighId) - 1;
            }
        };
    }

    /**
     * Returns the name of this store.
     *
     * @return The name of this store
     */
    @Override
    public Path getStorageFile() {
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
    private void openIdGenerator(CursorContextFactory contextFactory) throws IOException {
        idGenerator = idGeneratorFactory.open(
                pageCache,
                idFile,
                getIdType(),
                () -> {
                    try (var cursorContext = contextFactory.create("highIdScan")) {
                        return scanForHighId(cursorContext);
                    }
                },
                recordFormat.getMaxId(),
                readOnly,
                configuration,
                contextFactory,
                openOptions,
                SINGLE_IDS);
    }

    /**
     * Starts from the end of the file and scans backwards to find the highest in use record.
     * Can be used even if {@link #start(CursorContext)} hasn't been called. Basically this method should be used
     * over {@link #getHighestPossibleIdInUse(CursorContext)} and {@link IdGenerator#getHighId()} in cases where a store has been opened
     * but is in a scenario where recovery isn't possible, like some tooling or migration.
     *
     * @return the id of the highest in use record + 1, i.e. highId.
     */
    protected long scanForHighId(CursorContext cursorContext) {
        try (PageCursor cursor = pagedFile.io(0, PF_SHARED_READ_LOCK | PF_READ_AHEAD, cursorContext)) {
            int recordsPerPage = getRecordsPerPage();
            int recordSize = getRecordSize();

            // Scan pages backwards in the chunk using page cache prefetch (PF_READ_AHEAD)
            for (long currentId = pagedFile.getLastPageId(); currentId >= 0 && cursor.next(currentId); currentId--) {
                long highestId = 0;
                boolean found;
                do {
                    found = false;
                    long basePageId = cursor.getCurrentPageId() * recordsPerPage;
                    for (int record = 0; record < recordsPerPage; record++) {
                        cursor.setOffset(record * recordSize);
                        if (isInUse(cursor)) {
                            // We've found the highest id in use
                            highestId = basePageId + record + 1;
                            found = true;
                        }
                    }
                } while (cursor.shouldRetry());

                checkIdScanCursorBounds(cursor);
                if (found) {
                    return highestId;
                }
            }

            return getNumberOfReservedLowIds();
        } catch (IOException e) {
            throw new UnderlyingStorageException("Unable to find high id by scanning backwards " + getStorageFile(), e);
        }
    }

    protected int determineRecordSize() {
        return recordFormat.getRecordSize(storeHeader);
    }

    @Override
    public final int getRecordSize() {
        return recordSize;
    }

    public long getStoreSize() throws IOException {
        return pagedFile.fileSize();
    }

    @Override
    public int getRecordDataSize() {
        return recordSize - recordFormat.getRecordHeaderSize();
    }

    private boolean isInUse(PageCursor cursor) {
        return recordFormat.isInUse(cursor);
    }

    @Override
    public void flush(FileFlushEvent flushEvent, CursorContext cursorContext) {
        try {
            pagedFile.flushAndForce(flushEvent);
            idGenerator.checkpoint(flushEvent, cursorContext);
        } catch (IOException e) {
            throw new UnderlyingStorageException("Failed to flush", e);
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
    public void close() {
        try {
            closeStoreFile();
        } catch (IllegalStateException e) {
            throw new UnderlyingStorageException("Failed to close store file: " + getStorageFile(), e);
        }
    }

    private void closeStoreFile() {
        Runnables.runAll(
                "Failure closing store and/or id generator",
                () -> {
                    if (pagedFile != null) {
                        pagedFile.close();
                        pagedFile = null;
                    }
                },
                () -> {
                    if (idGenerator != null) {
                        idGenerator.close();
                        idGenerator = null;
                    }
                });
    }

    /** @return The highest possible id in use, -1 if no id in use. */
    @Override
    public long getHighestPossibleIdInUse(CursorContext cursorContext) {
        return idGenerator != null ? idGenerator.getHighestPossibleIdInUse() : scanForHighId(cursorContext) - 1;
    }

    /**
     * @return the number of records at the beginning of the store file that are reserved for other things
     * than actual records. Stuff like permanent configuration data.
     */
    @Override
    public int getNumberOfReservedLowIds() {
        return storeHeaderFormat.numberOfReservedRecords();
    }

    public IdType getIdType() {
        return idType;
    }

    void logIdUsage(DiagnosticsLogger logger, CursorContext cursorContext) {
        logger.log(format(
                "%s[%s]: used=%s high=%s",
                getTypeDescriptor(),
                getStorageFile().getFileName(),
                getIdGenerator().getHighId(),
                getHighestPossibleIdInUse(cursorContext)));
    }

    @Override
    public RECORD newRecord() {
        return recordFormat.newRecord();
    }

    @Override
    public RECORD getRecordByCursor(
            long id, RECORD record, RecordLoad mode, PageCursor cursor, MemoryTracker memoryTracker)
            throws UnderlyingStorageException {
        try {
            readIntoRecord(id, record, mode, cursor, memoryTracker);
            return record;
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    private void readIntoRecord(long id, RECORD record, RecordLoad mode, PageCursor cursor, MemoryTracker memoryTracker)
            throws IOException {
        // Mark the record with this id regardless of whether or not we load the contents of it.
        // This is done in this method since there are multiple call sites and they all want the id
        // on that record, so it's to ensure it isn't forgotten.
        record.setId(id);
        long pageId = pageIdForRecord(id);
        int offset = offsetForId(id);
        if (cursor.next(pageId)) {
            cursor.setOffset(offset);
            readRecordFromPage(id, record, mode, cursor, memoryTracker);
        } else {
            verifyAfterNotRead(record, mode);
        }
    }

    @Override
    public void nextRecordByCursor(RECORD record, RecordLoad mode, PageCursor cursor, MemoryTracker memoryTracker)
            throws UnderlyingStorageException {
        if (cursor.getCurrentPageId() < -1) {
            throw new IllegalArgumentException("Pages are assumed to be positive or -1 if not initialized");
        }

        try {
            long id = record.getId() + 1;
            record.setId(id);
            long pageId = cursor.getCurrentPageId();
            if ((cursor.getOffset() >= recordsEndOffset) || (pageId < 0)) {
                if (!cursor.next()) {
                    verifyAfterNotRead(record, mode);
                    return;
                }
            }
            readRecordFromPage(id, record, mode, cursor, memoryTracker);
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    private void readRecordFromPage(
            long id, RECORD record, RecordLoad mode, PageCursor cursor, MemoryTracker memoryTracker)
            throws IOException {
        cursor.mark();
        do {
            prepareForReading(cursor, record);
            recordFormat.read(record, cursor, mode, recordSize, recordsPerPage, memoryTracker);
        } while (cursor.shouldRetry());
        checkForDecodingErrors(cursor, id, mode);
        verifyAfterReading(record, mode);
    }

    @Override
    public void updateRecord(
            RECORD record,
            IdUpdateListener idUpdateListener,
            PageCursor cursor,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        long id = record.getId();
        IdValidator.assertValidId(getIdType(), id, recordFormat.getMaxId());

        long pageId = pageIdForRecord(id);
        int offset = offsetForId(id);
        try {
            if (cursor.next(pageId)) {
                cursor.setOffset(offset);
                recordFormat.write(record, cursor, recordSize, recordsPerPage);
                checkForDecodingErrors(cursor, id, NORMAL); // We don't free ids if something weird goes wrong
                if (!record.inUse()) {
                    idUpdateListener.markIdAsUnused(idGenerator, id, 1, cursorContext);
                } else if (record.isCreated()) {
                    idUpdateListener.markIdAsUsed(idGenerator, id, 1, cursorContext);
                }

                if ((!record.inUse() || !record.requiresSecondaryUnit()) && record.hasSecondaryUnitId()) {
                    // If record was just now deleted, or if the record used a secondary unit, but not anymore
                    // then free the id of that secondary unit.
                    idUpdateListener.markIdAsUnused(idGenerator, record.getSecondaryUnitId(), 1, cursorContext);
                }
                if (record.inUse() && record.isSecondaryUnitCreated()) {
                    // Triggers on:
                    // - (a) record got created right now and has a secondary unit, or
                    // - (b) it already existed and just now grew into a secondary unit then mark the secondary unit as
                    // used
                    idUpdateListener.markIdAsUsed(idGenerator, record.getSecondaryUnitId(), 1, cursorContext);
                }
            }
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    @Override
    public void prepareForCommit(RECORD record, IdSequence idSequence, CursorContext cursorContext) {
        if (record.inUse()) {
            recordFormat.prepare(record, recordSize, idSequence, cursorContext);
        }
    }

    @Override
    public <EXCEPTION extends Exception> void scanAllRecords(
            Visitor<RECORD, EXCEPTION> visitor, PageCursor pageCursor, MemoryTracker memoryTracker) throws EXCEPTION {
        RECORD record = newRecord();
        long highId = getIdGenerator().getHighId();
        for (long id = getNumberOfReservedLowIds(); id < highId; id++) {
            getRecordByCursor(id, record, LENIENT_CHECK, pageCursor, memoryTracker);
            if (record.inUse()) {
                visitor.visit(record);
            }
        }
    }

    private void verifyAfterNotRead(RECORD record, RecordLoad mode) {
        record.clear();
        mode.verify(record);
    }

    final void checkForDecodingErrors(PageCursor cursor, long recordId, RecordLoad mode) {
        if (mode.checkForOutOfBounds(cursor)) {
            throwOutOfBoundsException(recordId);
        }
        mode.clearOrThrowCursorError(cursor);
    }

    private void throwOutOfBoundsException(long recordId) {
        RECORD record = newRecord();
        record.setId(recordId);
        long pageId = pageIdForRecord(recordId);
        int offset = offsetForId(recordId);
        throw new UnderlyingStorageException(buildOutOfBoundsExceptionMessage(
                record,
                pageId,
                offset,
                recordSize,
                pagedFile.pageSize(),
                storageFile.toAbsolutePath().toString()));
    }

    static String buildOutOfBoundsExceptionMessage(
            AbstractBaseRecord record, long pageId, int offset, int recordSize, int pagePayload, String filename) {
        return "Access to record " + record + " went out of bounds of the page. The record size is " + recordSize
                + " bytes, and the access was at offset " + offset + " bytes into page " + pageId
                + ", and the pages have a capacity of " + pagePayload + " bytes. "
                + "The mapped store file in question is "
                + filename;
    }

    private void verifyAfterReading(RECORD record, RecordLoad mode) {
        if (!mode.verify(record)) {
            record.clear();
        }
    }

    private void prepareForReading(PageCursor cursor, RECORD record) {
        // Mark this record as unused. This to simplify implementations of readRecord.
        // readRecord can behave differently depending on RecordLoad argument and so it may be that
        // contents of a record may be loaded even if that record is unused, where the contents
        // can still be initialized data. Know that for many record stores, deleting a record means
        // just setting one byte or bit in that record.
        record.setInUse(false);
        cursor.setOffsetToMark();
    }

    @Override
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    @Override
    public void ensureHeavy(RECORD record, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        // Do nothing by default. Some record stores have this.
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public int getStoreHeaderInt() {
        return ((IntStoreHeader) storeHeader).value();
    }

    @Override
    public void allocate(long highId) throws IOException {
        pagedFile.preAllocate(pageIdForRecord(highId));
    }

    @Override
    public long estimateAvailableReservedSpace() {
        // This doesn't account for unallocated records in the last page whose IDs are greater than highId,
        // which means it is only accurate up to the page size. This is fine since it's a conservative estimate.
        return idGenerator.getUnusedIdCount() * recordSize;
    }
}
