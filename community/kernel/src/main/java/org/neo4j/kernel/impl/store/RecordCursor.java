/*
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
package org.neo4j.kernel.impl.store;

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
     * Initialize this cursor, placing it at record {@code id} and using the given {@link PageCursor} and
     * {@link RecordLoad} for loading record data.
     *
     * @param id record id to start at. Records chains can be followed using
     * {@link RecordStore#getNextRecordReference(AbstractBaseRecord)}.
     * @param mode {@link RecordLoad} for loading.
     * @param pageCursor {@link PageCursor} for the source of the data.
     * @return this record cursor.
     */
    RecordCursor<R> init( long id, RecordLoad mode, PageCursor pageCursor );

    /**
     * An additional way of placing this cursor at an arbitrary record id.
     *
     * @param id record id to place cursor at.
     * @return whether or not that record is in use.
     */
    boolean next( long id );
}
