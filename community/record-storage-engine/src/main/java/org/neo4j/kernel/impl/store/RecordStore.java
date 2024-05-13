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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;

/**
 * A store for {@link #updateRecord(AbstractBaseRecord, PageCursor, CursorContext, StoreCursors) updating} and
 * {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)} getting} records.
 *
 * There are two ways of getting records, either one-by-one using
 * {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}, passing in record retrieved from {@link #newRecord()}.
 * This to make a conscious decision about who will create the record instance and in that process figure out
 * ways to reduce number of record instances created.
 * <p>
 * The other way is to use {@link #openPageCursorForReading(long, CursorContext)} to open a cursor and use it to read records using
 * {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}. A {@link PageCursor} can be ket open
 * to read multiple records before closing it.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}.
 */
public interface RecordStore<RECORD extends AbstractBaseRecord> {
    /**
     * @return the {@link Path} that backs this store.
     */
    Path getStorageFile();

    IdGenerator getIdGenerator();

    /**
     * Checks for highest possible id in use in id generator (if that is already available) otherwise performs store scan.
     * @param cursorContext underlying page cursor context.
     * @return highest id in use in this store.
     */
    long getHighestPossibleIdInUse(CursorContext cursorContext);

    /**
     * Some stores may have meta data stored in the header of the store file. Since all records in a store
     * are of the same size the means of storing that meta data is to occupy one or more records at the
     * beginning of the store (0...).
     *
     * @return the number of records in the beginning of the file that are reserved for header meta data.
     */
    int getNumberOfReservedLowIds();

    /**
     * Returns store header (see {@link #getNumberOfReservedLowIds()}) as {@code int}. Exposed like this
     * for convenience since all known store headers are ints.
     *
     * @return store header as an int value, e.g the first 4 bytes of the first (reserved) record in this store.
     */
    int getStoreHeaderInt();

    /**
     * @return a new record instance for receiving data by {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}.
     */
    RECORD newRecord();

    /**
     * Opens a {@link PageCursor} on this store, capable of reading records using
     * {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}.
     * The caller is responsible for closing it when done with it.
     *
     * @param id cursor will initially be placed at the page containing this record id.
     * @param cursorContext underlying page cursor context.
     * @return PageCursor for reading records.
     */
    PageCursor openPageCursorForReading(long id, CursorContext cursorContext);

    /**
     * Opens a {@link PageCursor} on this store, capable of reading only multi versioned record chain heads.
     * The caller is responsible for closing it when done with it.
     *
     * @param id cursor will initially be placed at the page containing this record id.
     * @param cursorContext underlying page cursor context.
     * @return PageCursor for reading head chain records and pages
     */
    PageCursor openPageCursorForReadingHeadOnly(long id, CursorContext cursorContext);

    /**
     * Opens a {@link PageCursor} on this store, capable of reading records using
     * {@link #getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}.
     * The caller is responsible for closing it when done with it.
     * The opened cursor will make use of pre-fetching for optimal scanning performance.
     *
     * @param id cursor will initially be placed at the page containing this record id.
     * @param cursorContext underlying page cursor context.
     * @return PageCursor for reading records.
     */
    PageCursor openPageCursorForReadingWithPrefetching(long id, CursorContext cursorContext);

    /**
     * Opens a {@link PageCursor} on this store, capable of writing records using
     * {@link #updateRecord(AbstractBaseRecord, PageCursor, CursorContext, StoreCursors)}.
     * The caller is responsible for closing it when done with it.
     *
     * @param id cursor will initially be placed at the page containing this record id.
     * @param cursorContext underlying page cursor context.
     * @return PageCursor for writing records.
     */
    PageCursor openPageCursorForWriting(long id, CursorContext cursorContext);

    /**
     * Reads a record from the store into {@code target}. Depending on {@link RecordLoad} given there will
     * be different behavior, although the {@code target} record will be marked with the specified
     * {@code id} after participating in this method call.
     * <ul>
     * <li>{@link RecordLoad#CHECK}: As little data as possible is read to determine whether the record
     *     is in use or not. If not in use then no more data will be loaded into the target record and
     *     the data of the record will be {@link AbstractBaseRecord#clear() cleared}.</li>
     * <li>{@link RecordLoad#NORMAL}: Just like {@link RecordLoad#CHECK}, but with the difference that
     *     an {@link InvalidRecordException} will be thrown if the record isn't in use.</li>
     * <li>{@link RecordLoad#FORCE}: The entire contents of the record will be loaded into the target record
     *     regardless if the record is in use or not. This leaves no guarantees about the data in the record
     *     after this method call, except that the id will be the specified {@code id}.
     * <li>{@link RecordLoad#ALWAYS}: Similar to {@link RecordLoad#FORCE}, except the sanity checks on
     *     the record data is always enabled.</li>
     *
     * The provided page cursor will be used to get the record, and in doing this it will be redirected to the
     * correct page if needed.
     *
     * @param id the id of the record to load.
     * @param target record where data will be loaded into. This record will have its id set to the specified
     * {@code id} as part of this method call.
     * @param mode loading behaviour, read more in method description.
     * @param cursor the PageCursor to use for record loading.
     * @throws InvalidRecordException if record not in use and the {@code mode} allows for throwing.
     */
    RECORD getRecordByCursor(long id, RECORD target, RecordLoad mode, PageCursor cursor) throws InvalidRecordException;

    /**
     * Reads a record from the store into {@code target}, see
     * {@link RecordStore#getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}.
     * <p>
     * This method requires that the cursor page and offset point to the first byte of the record in target on calling.
     * The provided page cursor will be used to get the record, and in doing this it will be redirected to the
     * next page if the input record was the last on it's page.
     *
     * @param target the record to fill.
     * @param mode loading behaviour, read more in {@link RecordStore#getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)}.
     * @param cursor pageCursor to use for record loading.
     * @throws InvalidRecordException if record not in use and the {@code mode} allows for throwing.
     */
    void nextRecordByCursor(RECORD target, RecordLoad mode, PageCursor cursor) throws InvalidRecordException;

    /**
     * For stores that have other stores coupled underneath, the "top level" record will have a flag
     * saying whether or not it's light. Light means that no records from the coupled store have been loaded yet.
     * This method can load those records and enrich the target record with those, marking it as heavy.
     *
     * @param record record to make heavy, if not already.
     * @param storeCursors pageCursor provider to be used for record loading.
     */
    void ensureHeavy(RECORD record, StoreCursors storeCursors);

    /**
     * Reads records that belong together, a chain of records that as a whole forms the entirety of a data item.
     *
     * @param firstId record id of the first record to start loading from.
     * @param mode {@link RecordLoad} mode.
     * @param guardForCycles Set to {@code true} if we need to take extra care in guarding for cycles in the chain.
     * When a cycle is found, a {@link RecordChainCycleDetectedException} will be thrown.
     * If {@code false}, then chain cycles will likely end up causing an {@link OutOfMemoryError}.
     * A cycle would only occur if the store is inconsistent, though.
     * @param pageCursor page cursor to be used for record reading
     * @return {@link Collection} of records in the loaded chain.
     * @throws InvalidRecordException if some record not in use and the {@code mode} is allows for throwing.
     */
    List<RECORD> getRecords(long firstId, RecordLoad mode, boolean guardForCycles, PageCursor pageCursor)
            throws InvalidRecordException;

    /**
     * Streams records that belong together, a chain of records that as a whole forms the entirety of a data item.
     *
     * @param firstId record id of the first record to start loading from.
     * @param mode {@link RecordLoad} mode.
     * @param guardForCycles Set to {@code true} if we need to take extra care in guarding for cycles in the chain.
     * When a cycle is found, a {@link RecordChainCycleDetectedException} will be thrown.
     * If {@code false}, then chain cycles will likely end up causing an {@link OutOfMemoryError}.
     * A cycle would only occur if the store is inconsistent, though.
     * @param pageCursor page cursor to be used for record reading
     * @param subscriber The subscriber of the data, will receive records until the subscriber returns <code>false</code>
     */
    void streamRecords(
            long firstId,
            RecordLoad mode,
            boolean guardForCycles,
            PageCursor pageCursor,
            RecordSubscriber<RECORD> subscriber);

    /**
     * Updates this store with the contents of {@code record} at the record id
     * {@link AbstractBaseRecord#getId() specified} by the record. The whole record will be written if
     * the given record is {@link AbstractBaseRecord#inUse() in use}, not necessarily so if it's not in use.
     *
     * @param record containing data to write to this store at the {@link AbstractBaseRecord#getId() id}
     * specified by the record.
     * @param cursorContext underlying page cursor context.
     */
    void updateRecord(
            RECORD record,
            IdUpdateListener idUpdates,
            PageCursor cursor,
            CursorContext cursorContext,
            StoreCursors storeCursors);

    default void updateRecord(
            RECORD record, PageCursor cursor, CursorContext cursorContext, StoreCursors storeCursors) {
        updateRecord(record, IdUpdateListener.DIRECT, cursor, cursorContext, storeCursors);
    }

    /**
     * @return number of bytes each record in this store occupies. All records in a store is of the same size.
     */
    int getRecordSize();

    /**
     * @return record "data" size, only applicable to dynamic record stores where record size may be specified
     * at creation time and later used every time the store is opened. Data size refers to number of bytes
     * of a record without header information, such as "inUse" and "next".
     */
    int getRecordDataSize();

    /**
     * @return underlying storage is assumed to work with pages. This method returns number of records that
     * will fit into each page.
     */
    int getRecordsPerPage();

    /**
     * Closes this store and releases any resource attached to it.
     */
    void close();

    /**
     * Flushes all pending {@link #updateRecord(AbstractBaseRecord, PageCursor, CursorContext, StoreCursors) updates} to underlying storage.
     * This call is blocking and will ensure all updates since last call to this method are durable
     * once the call returns.
     */
    void flush(FileFlushEvent flushEvent, CursorContext cursorContext);

    /**
     * Called once all changes to a record is ready to be converted into a command.
     *
     * @param record record to prepare, potentially updating it with more information before converting into a command.
     * @param idSequence {@link IdSequence} to use for potentially generating additional ids required by this record.
     * @param cursorContext underlying page cursor context
     */
    void prepareForCommit(RECORD record, IdSequence idSequence, CursorContext cursorContext);

    /**
     * Scan the given range of records both inclusive, and pass all the in-use ones to the given processor, one by one.
     *
     * The record passed to the NodeRecordScanner is reused instead of reallocated for every record, so it must be
     * cloned if you want to save it for later.
     * @param visitor {@link Visitor} notified about all records.
     * @param pageCursor pageCursor to use for record reading.
     * @throws EXCEPTION on error reading from store.
     */
    <EXCEPTION extends Exception> void scanAllRecords(Visitor<RECORD, EXCEPTION> visitor, PageCursor pageCursor)
            throws EXCEPTION;

    /**
     * Send a hint to the file system that it may reserve at least the given number of pages worth of capacity
     * for this file.
     * The operation throws {@link IOException} if the operation fails. The users may choose to specifically handle
     * {@link OutOfDiskSpaceException} as detecting low disk space in advance is one of the main purposes
     * of this operation.
     */
    void allocate(long highId) throws IOException;

    /**
     * Conservatively estimate how much reserved space is available for (re)use.
     * @return available reserved space estimate in bytes
     * @throws IOException on error reading from store.
     */
    long estimateAvailableReservedSpace();
}
