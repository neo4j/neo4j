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
package org.neo4j.counts;

import java.io.IOException;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

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
    void start( CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker ) throws IOException;

    /**
     * Checkpoints changes made up until this point so that they are available even after next restart.
     *
     * @param cursorContext page cache access context.
     * @throws IOException on I/O error.
     */
    void checkpoint( CursorContext cursorContext ) throws IOException;
}
