/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

public interface ReadableRaftLog
{
    /**
     * @return The index of the last appended entry.
     */
    long appendIndex();

    /**
     * @return The index immediately preceding entries in the log.
     */
    long prevIndex();

    /**
     * Reads the term associated with the entry at the supplied index.
     *
     * @param logIndex The index of the log entry.
     * @return The term of the entry, or -1 if the entry does not exist
     */
    long readEntryTerm( long logIndex ) throws IOException;

    /**
     * Returns a {@link RaftLogCursor} of {@link RaftLogEntry}s from the specified index until the end of the log
     * @param fromIndex The log index at which the cursor should be positioned
     */
    RaftLogCursor getEntryCursor( long fromIndex ) throws IOException;
}
