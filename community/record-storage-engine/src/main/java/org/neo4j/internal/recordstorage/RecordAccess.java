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
package org.neo4j.internal.recordstorage;

import java.util.Collection;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * Provides access to records, both for reading and for writing.
 */
public interface RecordAccess<RECORD,ADDITIONAL>
{
    /**
     * Gets an already loaded record, or loads it as part of this call if it wasn't. The {@link RecordProxy}
     * returned has means of communicating when to get access to the actual record for reading or writing.
     * With that information any additional loading or storing can be inferred for the specific
     * use case (implementation).
     *
     * @param key the record key.
     * @param additionalData additional data to put in the record after loaded.
     * @return a {@link RecordProxy} for the record for {@code key}.
     */
    RecordProxy<RECORD, ADDITIONAL> getOrLoad( long key, ADDITIONAL additionalData, PageCursorTracer cursorTracer );

    RecordProxy<RECORD, ADDITIONAL> getIfLoaded( long key );

    RecordProxy<RECORD,ADDITIONAL> setRecord( long key, RECORD record, ADDITIONAL additionalData, PageCursorTracer cursorTracer );

    /**
     * Creates a new record with the given {@code key}. Any {@code additionalData} is set in the
     * record before returning.
     *
     * @param key the record key.
     * @param additionalData additional data to put in the record after loaded.
     * @return a {@link RecordProxy} for the record for {@code key}.
     */
    RecordProxy<RECORD, ADDITIONAL> create( long key, ADDITIONAL additionalData, PageCursorTracer cursorTracer );

    int changeSize();

    Collection<? extends RecordProxy<RECORD,ADDITIONAL>> changes();

    /**
     * A proxy for a record that encapsulates load/store actions to take, knowing when the underlying record is
     * requested for reading or for writing.
     */
    interface RecordProxy<RECORD, ADDITIONAL>
    {
        long getKey();

        RECORD forChangingLinkage();

        RECORD forChangingData();

        RECORD forReadingLinkage();

        RECORD forReadingData();

        ADDITIONAL getAdditionalData();

        RECORD getBefore();

        boolean isChanged();

        boolean isCreated();
    }

    /**
     * Hook for loading and creating records.
     */
    interface Loader<RECORD,ADDITIONAL>
    {
        RECORD newUnused( long key, ADDITIONAL additionalData );

        RECORD load( long key, ADDITIONAL additionalData, PageCursorTracer cursorTracer );

        void ensureHeavy( RECORD record, PageCursorTracer cursorTracer );

        RECORD copy( RECORD record );
    }
}
