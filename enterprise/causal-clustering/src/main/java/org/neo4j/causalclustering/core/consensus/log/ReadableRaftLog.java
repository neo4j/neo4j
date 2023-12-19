/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
