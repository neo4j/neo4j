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
package org.neo4j.coreedge.raft.state.id_allocation;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.neo4j.coreedge.raft.replication.id.IdAllocationState;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.kernel.impl.store.id.IdType;

import static java.util.Arrays.copyOf;

/**
 * An in-memory representation of the IDs allocated to this core instance.
 * Instances of this class are serialized to disk by
 *
 * {@link InMemoryIdAllocationState.InMemoryIdAllocationStateChannelMarshal}. The serialized form:
 * <p>
 * +----------------------------------+
 * |  8-byte length marker            |
 * +----------------------------------+
 * |  first unallocated               |
 * |  15x 8-byte                      |
 * +----------------------------------+
 * |  8-byte length marker            |
 * +----------------------------------+
 * |  previous id range start         |
 * |  15x 8-byte                      |
 * +----------------------------------+
 * |  8-byte length marker            |
 * +----------------------------------+
 * |  previous id range length        |
 * |  15x 8-byte                      |
 * +----------------------------------+
 */
public class InMemoryIdAllocationState implements IdAllocationState, Serializable
{
    private final long[] firstUnallocated;
    private final long[] lastIdRangeStartForMe;
    private final int[] lastIdRangeLengthForMe;
    private long logIndex;

    public InMemoryIdAllocationState()
    {
        this( new long[IdType.values().length],
                new long[IdType.values().length],
                new int[IdType.values().length],
                -1L );
    }

    private InMemoryIdAllocationState( long[] firstUnallocated,
                                       long[] lastIdRangeStartForMe,
                                       int[] lastIdRangeLengthForMe,
                                       long logIndex )
    {
        this.firstUnallocated = firstUnallocated;
        this.lastIdRangeStartForMe = lastIdRangeStartForMe;
        this.lastIdRangeLengthForMe = lastIdRangeLengthForMe;
        this.logIndex = logIndex;
    }

    public InMemoryIdAllocationState( InMemoryIdAllocationState other )
    {
        this.firstUnallocated = copyOf( other.firstUnallocated, other.firstUnallocated.length );
        this.lastIdRangeStartForMe = copyOf( other.lastIdRangeStartForMe, other.lastIdRangeStartForMe.length );
        this.lastIdRangeLengthForMe = copyOf( other.lastIdRangeLengthForMe, other.lastIdRangeLengthForMe.length );
        this.logIndex = other.logIndex;
    }

    @Override
    public int lastIdRangeLength( IdType idType )
    {
        return lastIdRangeLengthForMe[idType.ordinal()];
    }

    @Override
    public void lastIdRangeLength( IdType idType, int idRangeLength )
    {
        lastIdRangeLengthForMe[idType.ordinal()] = idRangeLength;
    }

    @Override
    public long logIndex()
    {
        return logIndex;
    }

    @Override
    public void logIndex( long logIndex )
    {
        this.logIndex = logIndex;
    }

    @Override
    public long firstUnallocated( IdType idType )
    {
        return firstUnallocated[idType.ordinal()];
    }

    @Override
    public void firstUnallocated( IdType idType, long idRangeEnd )
    {
        firstUnallocated[idType.ordinal()] = idRangeEnd;
    }

    @Override
    public long lastIdRangeStart( IdType idType )
    {
        return lastIdRangeStartForMe[idType.ordinal()];
    }

    @Override
    public void lastIdRangeStart( IdType idType, long idRangeStart )
    {
        lastIdRangeStartForMe[idType.ordinal()] = idRangeStart;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InMemoryIdAllocationState that = (InMemoryIdAllocationState) o;

        if ( logIndex != that.logIndex )
        {
            return false;
        }
        if ( !Arrays.equals( firstUnallocated, that.firstUnallocated ) )
        {
            return false;
        }
        if ( !Arrays.equals( lastIdRangeStartForMe, that.lastIdRangeStartForMe ) )
        {
            return false;
        }
        return Arrays.equals( lastIdRangeLengthForMe, that.lastIdRangeLengthForMe );
    }

    @Override
    public int hashCode()
    {
        int result = Arrays.hashCode( firstUnallocated );
        result = 31 * result + Arrays.hashCode( lastIdRangeStartForMe );
        result = 31 * result + Arrays.hashCode( lastIdRangeLengthForMe );
        result = 31 * result + (int) (logIndex ^ (logIndex >>> 32));
        return result;
    }

    static class InMemoryIdAllocationStateChannelMarshal implements ChannelMarshal<InMemoryIdAllocationState>
    {
        public static final int NUMBER_OF_BYTES_PER_WRITE =
                3 * IdType.values().length * 8 // 3 arrays of IdType enum value length storing longs
                        + 8 * 3 // the length (as long) for each array
                        + 8; // the raft log index

        @Override
        public void marshal( InMemoryIdAllocationState state, WritableChannel channel ) throws IOException
        {
            channel.putLong( (long) state.firstUnallocated.length );
            for ( long l : state.firstUnallocated )
            {
                channel.putLong( l );
            }

            channel.putLong( (long) state.lastIdRangeStartForMe.length );
            for ( long l : state.lastIdRangeStartForMe )
            {
                channel.putLong( l );
            }

            channel.putLong( state.lastIdRangeLengthForMe.length );
            for ( int i : state.lastIdRangeLengthForMe )
            {
                channel.putLong( i );
            }
            channel.putLong( state.logIndex );
        }

        @Override
        public InMemoryIdAllocationState unmarshal( ReadableChannel channel ) throws IOException
        {
            try
            {
                long[] firstNotAllocated = new long[(int) channel.getLong()];

                for ( int i = 0; i < firstNotAllocated.length; i++ )
                {
                    firstNotAllocated[i] = channel.getLong();
                }

                long[] lastIdRangeStartForMe = new long[(int) channel.getLong()];
                for ( int i = 0; i < lastIdRangeStartForMe.length; i++ )
                {
                    lastIdRangeStartForMe[i] = channel.getLong();
                }

                int[] lastIdRangeLengthForMe = new int[(int) channel.getLong()];
                for ( int i = 0; i < lastIdRangeLengthForMe.length; i++ )
                {
                    lastIdRangeLengthForMe[i] = (int) channel.getLong();
                }

                long logIndex = channel.getLong();

                return new InMemoryIdAllocationState( firstNotAllocated, lastIdRangeStartForMe,
                        lastIdRangeLengthForMe, logIndex );
            }
            catch ( ReadPastEndException ex )
            {
                return null;
            }
        }
    }
}
