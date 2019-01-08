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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.cursor.Cursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

/**
 * {@link Cursor} over {@link AbstractBaseRecord} read from a {@link RecordStore}.
 * Reading multiple records from a store will be more efficient with a {@link RecordCursor}
 * than calling {@link RecordStore#getRecord(long, AbstractBaseRecord, RecordLoad)} one by one
 * since a {@link PageCursor} is held open until {@link #close()} is called and one and the same
 * record instances is used in every call to {@link #next()}.
 *
 * @param <R> type of {@link AbstractBaseRecord}.
 */
public interface RecordCursor<R extends AbstractBaseRecord> extends Cursor<R>
{
    /**
     * Acquires this cursor, placing it at record {@code id} {@link RecordLoad} for loading record data.
     *
     * @param id record id to start at.
     * @param mode {@link RecordLoad} for loading.
     * @return this record cursor.
     */
    RecordCursor<R> acquire( long id, RecordLoad mode );

    /**
     * Moves this cursor to the specified {@code id} with the specified {@link RecordLoad mode} without actually
     * fetching the record. {@link #next()} and {@link #get()} could be used next to fetch the record.
     *
     * @param id the id of the record.
     * @param mode the mode for subsequent loading.
     */
    void placeAt( long id, RecordLoad mode );

    /**
     * Moves to the next record and reads it. If this is the first call since {@link #acquire(long, RecordLoad)}
     * the record specified in acquire will be read, otherwise the next record in the chain,
     * {@link RecordStore#getNextRecordReference(AbstractBaseRecord)}.
     * The read record can be accessed using {@link #get()}.
     *
     * @return whether or not that record is in use.
     */
    @Override
    boolean next();

    /**
     * An additional way of placing this cursor at an arbitrary record id. This is useful when stride,
     * as opposed to following record chains, is controlled from the outside.
     * The read record can be accessed using {@link #get()}.
     *
     * @param id record id to place cursor at.
     * @return whether or not that record is in use.
     */
    boolean next( long id );

     /**
      * An additional way of placing this cursor at an arbitrary record id.
      * Calling this method will not advance the "current id" as to change which {@link #next()} will load next.
      * This method is useful when there's an opportunity to load a record from an already acquired
      * {@link PageCursor} and potentially even an already pinned page.
      *
      * @param id record id to place cursor at.
      * @param record record to load the record data into.
      * @param mode {@link RecordLoad} mode temporarily overriding the default provided in
      * {@link #acquire(long, RecordLoad)}.
      * @return whether or not that record is in use.
      */
    boolean next( long id, R record, RecordLoad mode );

    /**
     * Read all records in the chain starting from the id this cursor is positioned at using either
     * {@link #acquire(long, RecordLoad)} or {@link #placeAt(long, RecordLoad)}. Each next record in the chain is
     * determined by {@link RecordStore#getNextRecordReference(AbstractBaseRecord)}. Each record placed in the
     * resulting list is a clone of the reused record.
     *
     * @return records of the chain in list.
     */
    @SuppressWarnings( "unchecked" )
    default List<R> getAll()
    {
        List<R> recordList = new ArrayList<>();
        while ( next() )
        {
            recordList.add( (R) get().clone() );
        }
        return recordList;
    }

    class Delegator<R extends AbstractBaseRecord> implements RecordCursor<R>
    {
        private final RecordCursor<R> actual;

        public Delegator( RecordCursor<R> actual )
        {
            this.actual = actual;
        }

        @Override
        public R get()
        {
            return actual.get();
        }

        @Override
        public boolean next()
        {
            return actual.next();
        }

        @Override
        public void placeAt( long id, RecordLoad mode )
        {
            actual.placeAt( id, mode );
        }

        @Override
        public void close()
        {
            actual.close();
        }

        @Override
        public RecordCursor<R> acquire( long id, RecordLoad mode )
        {
            actual.acquire( id, mode );
            return this;
        }

        @Override
        public boolean next( long id )
        {
            return actual.next( id );
        }

        @Override
        public boolean next( long id, R record, RecordLoad mode )
        {
            return actual.next( id, record, mode );
        }
    }
}
