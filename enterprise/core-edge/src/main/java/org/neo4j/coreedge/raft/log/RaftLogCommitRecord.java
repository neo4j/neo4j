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

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.coreedge.raft.log.PhysicalRaftLog.RecordType.COMMIT;

/**
 * The commit record stores the latest commitIndex marking the appended entries
 * that are appended with consensus and are thus safe to apply.
 */
public class RaftLogCommitRecord extends RaftLogRecord
{
    private final long commitIndex;

    public RaftLogCommitRecord( long commitIndex )
    {
        super( COMMIT );
        this.commitIndex = commitIndex;
    }

    public long commitIndex()
    {
        return commitIndex;
    }

    public static RaftLogCommitRecord read( ReadableChannel channel ) throws IOException
    {
        long commitIndex = channel.getLong();
        return new RaftLogCommitRecord( commitIndex );
    }

    public static void write( WritableChannel channel, long commitIndex ) throws IOException
    {
        channel.put( COMMIT.value() );
        channel.putLong( commitIndex );
    }

    @Override
    public String toString()
    {
        return "RaftLogCommitRecord{" +
               "commitIndex=" + commitIndex +
               '}';
    }
}
