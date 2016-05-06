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
package org.neo4j.coreedge.raft.log.physical;

import java.io.IOException;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.coreedge.raft.log.physical.PhysicalRaftLog.RecordType.CONTINUATION;

/**
 * Continuation records are written at the beginning of a new log file
 * and define the start position (prevLogIndex+1) for subsequently appended
 * entries. At the same time prevLogIndex is thus the index of the last
 * valid entry in the previous file, and prevLogTerm its term. These values
 * are used for log matching.
 *
 * New log files are created when truncating, skipping or when the threshold
 * size for a single log file has been exceeded.
 */
public class RaftLogContinuationRecord extends RaftLogRecord
{
    private final long prevLogIndex;
    private final long prevLogTerm;

    RaftLogContinuationRecord( long prevLogIndex, long prevLogTerm )
    {
        super( CONTINUATION );
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
    }

    public long prevLogIndex()
    {
        return prevLogIndex;
    }

    public long prevLogTerm()
    {
        return prevLogTerm;
    }

    public static RaftLogContinuationRecord read( ReadableChannel channel ) throws IOException
    {
        long prevLogIndex = channel.getLong();
        long prevLogTerm = channel.getLong();

        return new RaftLogContinuationRecord( prevLogIndex, prevLogTerm );
    }

    public static void write( WritableChannel channel, long prevLogIndex, long prevLogTerm ) throws IOException
    {
        channel.put( CONTINUATION.value() );
        channel.putLong( prevLogIndex );
        channel.putLong( prevLogTerm );
    }

    @Override
    public String toString()
    {
        return "RaftLogContinuationRecord{" +
               "prevLogIndex=" + prevLogIndex +
               ", prevLogTerm=" + prevLogTerm +
               '}';
    }
}
