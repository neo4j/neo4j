/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;

public interface ReadableRaftLog
{
    /**
     * @return The index of the last appended entry.
     */
    long appendIndex();

    /**
     * @return The index of the last committed entry.
     */
    long commitIndex();

    /**
     * Reads the log entry at the supplied index.
     *
     * @param logIndex The index of the log entry.
     * @return The log entry.
     */
    RaftLogEntry readLogEntry( long logIndex ) throws RaftStorageException;

    /**
     * Reads the content of the log entry at the supplied index.
     *
     * @param logIndex The index of the log entry.
     * @return The log entry content.
     */
    ReplicatedContent readEntryContent( long logIndex ) throws RaftStorageException;

    /**
     * Reads the term associated with the entry at the supplied index.
     *
     * @param logIndex The index of the log entry.
     * @return The term of the entry, or -1 if the entry does not exist
     */
    long readEntryTerm( long logIndex ) throws RaftStorageException;

    /**
     * Tells if a entry exists in the log at the supplied index.
     *
     * @param logIndex The index of the log entry.
     * @return True if the entry exists, otherwise false.
     */
    boolean entryExists( long logIndex );
}
