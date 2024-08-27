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
package org.neo4j.kernel.impl.store.format;

import java.io.IOException;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;

/**
 * Specifies a particular {@link AbstractBaseRecord record} format, used to read and write records in a
 * {@link RecordStore} from and to a {@link PageCursor}.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord} this format handles.
 */
public interface RecordFormat<RECORD extends AbstractBaseRecord> {
    int NO_RECORD_SIZE = 1;

    /**
     * Instantiates a new record to use in {@link #read(AbstractBaseRecord, PageCursor, RecordLoad, int, int)}
     * and {@link #write(AbstractBaseRecord, PageCursor, int, int)}. Records may be reused, which is why the instantiation
     * is separated from reading and writing.
     *
     * @return a new record instance, usable in {@link #read(AbstractBaseRecord, PageCursor, RecordLoad, int, int)}
     * and {@link #write(AbstractBaseRecord, PageCursor, int, int)}.
     */
    RECORD newRecord();

    /**
     * Returns the record size for this format. Supplied here is the {@link StoreHeader store header} of the
     * owning store, which may contain data affecting the record size.
     *
     * @param storeHeader {@link StoreHeader} with header information from the store.
     * @return record size of records of this format and store.
     */
    int getRecordSize(StoreHeader storeHeader);

    /**
     * @return header size of records of this format. This is only applicable to {@link DynamicRecord}
     * format and may not need to be in this interface.
     */
    int getRecordHeaderSize();

    /**
     * Quickly determines whether or not record starting right at where the {@code cursor} is placed
     * is in use or not.
     *
     * @param cursor {@link PageCursor} to read data from, placed at the start of record to determine
     * in use status of.
     * @return whether or not the record at where the {@code cursor} is placed is in use.
     */
    boolean isInUse(PageCursor cursor);

    /**
     * Reads data from {@code cursor} of the format specified by this implementation into {@code record}.
     * The cursor is placed at the beginning of the record id, which also {@code record}
     * {@link AbstractBaseRecord#getId() refers to}.
     *
     * @param memoryTracker
     * @param record to put read data into, replacing any existing data in that record object.
     * @param cursor {@link PageCursor} to read data from.
     * @param mode {@link RecordLoad} mode of reading.
     * See {@link RecordStore#getRecordByCursor(long, AbstractBaseRecord, RecordLoad, PageCursor)} for more information.
     * @param recordSize size of records of this format. This is passed in like this since not all formats
     * know the record size in advance, but may be read from store header when opening the store.
     * @throws IOException on error reading.
     */
    void read(
            RECORD record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker)
            throws IOException;

    /**
     * Called when all changes about a record has been gathered
     * and before it's time to convert into a command. The original reason for introducing this is the
     * thing with record units, where we need to know whether or not a record will span two units
     * before even writing to the log as a command. The format is the pluggable entity which knows
     * about the format and therefore the potential length of it and can update the given record with
     * additional information which needs to be written to the command, carried back inside the record
     * itself.
     *
     * @param record record to prepare, potentially updating it with more information before converting
     * into a command.
     * @param recordSize size of each record.
     * @param idSequence source of new ids if such are required be generated.
     * @param cursorContext underlying page cursor context
     */
    void prepare(RECORD record, int recordSize, IdSequence idSequence, CursorContext cursorContext);

    /**
     * Writes record contents to the {@code cursor} in the format specified by this implementation.
     *
     * @param record containing data to write.
     * @param cursor {@link PageCursor} to write the record data into.
     * @param recordSize size of records of this format. This is passed in like this since not all formats
     * know the record size in advance, but may be read from store header when opening the store.
     * @param recordsPerPage number of records per page. All stores know in advance how many records of particular format can fit on a page.
     * @throws IOException on error writing.
     */
    void write(RECORD record, PageCursor cursor, int recordSize, int recordsPerPage) throws IOException;

    /**
     * @param record to obtain "next" reference from.
     * @return "next" reference of records of this type.
     */
    long getNextRecordReference(RECORD record);

    /**
     * Can be used to compare against another {@link RecordFormat}, returns {@code true} the format
     * represented by {@code obj} is the exact same as this format.
     *
     * @param otherFormat other format to compare with.
     * @return whether or not the other format is the same as this one.
     */
    @Override
    boolean equals(Object otherFormat);

    /**
     * To match {@link #equals(Object)}.
     */
    @Override
    int hashCode();

    /**
     * Maximum number that can be used to as id in specified format
     * @return maximum possible id
     */
    long getMaxId();

    /**
     * Page size of store file represented by current record format
     * @param pageSize page cache page size
     * @param recordSize store format record size
     * @return page size for file
     */
    int getFilePageSize(int pageSize, int recordSize);
}
