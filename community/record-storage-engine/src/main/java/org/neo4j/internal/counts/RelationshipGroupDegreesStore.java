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
package org.neo4j.internal.counts;

import java.io.IOException;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;

public interface RelationshipGroupDegreesStore extends AutoCloseable
{
    Updater apply( long txId, PageCursorTracer cursorTracer );

    long degree( long groupId, RelationshipDirection direction, PageCursorTracer cursorTracer );

    void start( PageCursorTracer cursorTracer, MemoryTracker memoryTracker ) throws IOException;

    @Override
    void close();

    void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer ) throws IOException;

    interface Updater extends AutoCloseable
    {
        @Override
        void close();

        void increment( long groupId, RelationshipDirection direction, long delta );
    }
}
