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
package org.neo4j.counts;

import java.io.IOException;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.memory.MemoryTracker;

public interface CountsStorage extends AutoCloseable, ConsistencyCheckable
{
    /**
     * Closes this counts store so that no more changes can be made and no more counts can be read.
     */
    @Override
    void close();

    /**
     * Puts the counts store in started state, i.e. after potentially recovery has been made. Any changes
     * before this call is made are considered recovery repairs from a previous non-clean shutdown.
     * @throws IOException any type of error happening when transitioning to started state.
     */
    void start( PageCursorTracer cursorTracer, MemoryTracker memoryTracker ) throws IOException;

    /**
     * Checkpoints changes made up until this point so that they are available even after next restart.
     *
     * @param ioLimiter for limiting I/O during checkpoint.
     * @param cursorTracer for tracing page cache access.
     * @throws IOException on I/O error.
     */
    void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer ) throws IOException;
}
