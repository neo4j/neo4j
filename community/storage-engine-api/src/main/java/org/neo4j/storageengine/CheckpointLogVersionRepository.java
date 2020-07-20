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
package org.neo4j.storageengine;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public interface CheckpointLogVersionRepository
{
    /**
     * Returns the current checkpoint log version.
     */
    long getCheckpointLogVersion();

    /**
     * Set checkpoint log version
     * @param version new current log version
     */
    void setCheckpointLogVersion( long version, PageCursorTracer cursorTracer );

    /**
     * Increments (making sure it is persisted on disk) and returns the latest checkpoint log version for this repository.
     * It does so atomically and can potentially block.
     * @param cursorTracer underlying page cursor tracer.
     * @return the latest checkpoint log version for this repository.
     */
    long incrementAndGetCheckpointLogVersion( PageCursorTracer cursorTracer );
}
