/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.cursor.IOCursor;

/**
 * The only responsibility of a {@link RaftEntryStore} is to provide {@link IOCursor}s that run over a series of
 * {@link RaftLogAppendRecord}s. The cursor is expected to start at the specified logIndex and run until the end of the
 * raft log.Semantics as to whether truncated entries are returned, the behaviour when the end is reached and other
 * details are purposefully left to the implementations.
 */
public interface RaftEntryStore
{
    IOCursor<RaftLogAppendRecord> getEntriesFrom( long logIndex ) throws IOException, RaftLogCompactedException;
}
