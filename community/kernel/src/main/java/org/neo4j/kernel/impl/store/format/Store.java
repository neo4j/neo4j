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
package org.neo4j.kernel.impl.store.format;

import java.io.IOException;

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * This is here temporarily, because it clashes with {@link org.neo4j.kernel.impl.store.Store}. Should be moved once
 * we're done refactoring away the old store layer.
 *
 * @param <RECORD>
 * @param <CURSOR>
 */
public interface Store<RECORD, CURSOR extends Store.RecordCursor> extends Lifecycle
{
    /** No flags */
    static int SF_NO_FLAGS = 0;

    /** Hint that the cursor will be used to perform a sequential scan. */
    static int SF_SCAN = 1;

    /** Instead of a cursor starting at the beginning of the store, have a cursor start at the end and move backwards */
    static int SF_REVERSE_CURSOR = 1 << 1;

    /**
     * Gives you a cursor for efficiently reading the store. The cursor you get back will always at least implement
     * the {@link org.neo4j.kernel.impl.store.format.Store.RecordCursor} interface, but most stores are expected to provide
     * richer cursor interfaces that allow reading individual fields of whatever record you are currently positioned at.
     */
    CURSOR cursor(int flags);

    /** Read the specified record. */
    RECORD read(long id) throws IOException;

    void write(RECORD record) throws IOException;

    /** Allocate a new free record id. */
    long allocate();

    /** Signal that a record id is no longer used, freeing it up for others. */
    void free(long id);

    /**
     * A cursor to navigate around records in a store. Note that you MUST use a retry pattern when reading from the
     * cursor. You may group a set of reads within one "retry block", but this block may not span reading multiple
     * records. The usage pattern should look like:
     *
     * do
     * {
     *     inUse = cursor.inUse();
     *     record = cursor.record();
     * } while(cursor.retry());
     *
     * This makes things somewhat cumbersome and verbose, but it is a requirement because the underlying paging system
     * can use optimistic locking, which means we always stand the risk of a retry requirement when doing reads.
     */
    interface RecordCursor<RECORD> extends AutoCloseable
    {
        // TODO: Note that we need some way to force the page-cache retry operation upstream here

        /** Read a full record from the current position. The returned instance is potentially the same
         *  as the one returned from other calls to this method. */
        RECORD reusedRecord();

        /** Read a full record from the current position. The returned instance is a new instance,
         *  unlike {@link #reusedRecord()}. */
        RECORD clonedRecord();

        /** Is the record at the current position in use? */
        boolean inUse();

        /** The id of the current record. */
        long recordId();

        /** Moves to an explicit record position, independent of if that position is in use or not. */
        boolean position( long id );

        /** Moves to the next in-use record, or returns false if there are no more records in the store. */
        boolean next() throws IOException;

        /** Indicates that a read needs to be retried, this MUST be used when reading with the cursor. */
        boolean shouldRetry() throws IOException;

        @Override
        void close();
    }
}
