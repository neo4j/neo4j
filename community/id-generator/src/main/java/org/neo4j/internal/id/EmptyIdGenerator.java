/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.id;

import java.io.IOException;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

public class EmptyIdGenerator implements IdGenerator
{
    public static final EmptyIdGenerator EMPTY_ID_GENERATOR = new EmptyIdGenerator();
    private static final int EMPTY_ID = -1;

    private EmptyIdGenerator()
    {
    }

    @Override
    public IdRange nextIdBatch( int size, boolean forceConsecutiveAllocation, PageCursorTracer cursorTracer )
    {
        return new IdRange( EMPTY_LONG_ARRAY, EMPTY_ID, EMPTY_ID );
    }

    @Override
    public void setHighId( long id )
    {
        // nothing
    }

    @Override
    public void markHighestWrittenAtHighId()
    {
        // nothing
    }

    @Override
    public long getHighestWritten()
    {
        return EMPTY_ID;
    }

    @Override
    public long getHighId()
    {
        return EMPTY_ID;
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return EMPTY_ID;
    }

    @Override
    public Marker marker( PageCursorTracer cursorTracer )
    {
        return NOOP_MARKER;
    }

    @Override
    public void close()
    {
        // nothing
    }

    @Override
    public long getNumberOfIdsInUse()
    {
        return -1;
    }

    @Override
    public long getDefragCount()
    {
        return -1;
    }

    @Override
    public void checkpoint( PageCursorTracer cursorTracer )
    {
        // nothing
    }

    @Override
    public void maintenance( boolean awaitOngoing, PageCursorTracer cursorTracer )
    {
        // nothing
    }

    @Override
    public void start( FreeIds freeIdsForRebuild, PageCursorTracer cursorTracer ) throws IOException
    {
        // nothing
    }

    @Override
    public void clearCache( PageCursorTracer cursorTracer )
    {
        // nothing
    }

    @Override
    public long nextId( PageCursorTracer cursorTracer )
    {
        return EMPTY_ID;
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
    {
        return false;
    }
}
