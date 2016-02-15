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
import org.neo4j.coreedge.network.Message;
import java.util.Arrays;

import org.neo4j.coreedge.raft.state.StateMarshal;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.util.Arrays.copyOf;

/**
 * An in-memory representation of the IDs allocated to this core instance.
 * Instances of this class are serialized to disk by
 * <p/>
 * {@link Marshal}. The serialized form:
 * <p/>
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
public class IdAllocationState
{
    private final long[] firstUnallocated;
    private final long[] lastIdRangeStartForMe;
    private final int[] lastIdRangeLengthForMe;
    private long logIndex;

    public IdAllocationState()
    {
        this( new long[IdType.values().length],
                new long[IdType.values().length],
                new int[IdType.values().length],
                -1L );
    }

    private IdAllocationState( long[] firstUnallocated,
                               long[] lastIdRangeStartForMe,
                               int[] lastIdRangeLengthForMe,
                               long logIndex )
    {
        this.firstUnallocated = firstUnallocated;
        this.lastIdRangeStartForMe = lastIdRangeStartForMe;
        this.lastIdRangeLengthForMe = lastIdRangeLengthForMe;
        this.logIndex = logIndex;
    }

    public IdAllocationState( IdAllocationState other )
    {
        this.firstUnallocated = copyOf( other.firstUnallocated, other.firstUnallocated.length );
        this.lastIdRangeStartForMe = copyOf( other.lastIdRangeStartForMe, other.lastIdRangeStartForMe.length );
        this.lastIdRangeLengthForMe = copyOf( other.lastIdRangeLengthForMe, other.lastIdRangeLengthForMe.length );
        this.logIndex = other.logIndex;
    }

    /**
     * @param idType The type of graph object whose ID is under allocation
     * @return the length of the last ID range allocated
     */
    public int lastIdRangeLength( IdType idType )
    {
        return lastIdRangeLengthForMe[idType.ordinal()];
    }

    /**
     * @param idType        The type of graph object whose ID is under allocation
     * @param idRangeLength the length of the ID range to be allocated
     */
    public void lastIdRangeLength( IdType idType, int idRangeLength )
    {
        lastIdRangeLengthForMe[idType.ordinal()] = idRangeLength;
    }

    /**
     * @return The last set log index, which is the value last passed to {@link #logIndex(long)}
     */
    public long logIndex()
    {
        return logIndex;
    }

    /**
     * Sets the last seen log index, which is the last log index at which a replicated value that updated this state
     * was encountered.
     *
     * @param logIndex The value to set as the last log index at which this state was updated
     */
    public void logIndex( long logIndex )
    {
        this.logIndex = logIndex;
    }

    /**
     * @param idType the type of graph object whose ID is under allocation
     * @return the first unallocated entry for idType
     */
    public long firstUnallocated( IdType idType )
    {
        return firstUnallocated[idType.ordinal()];
    }

    /**
     * @param idType     the type of graph object whose ID is under allocation
     * @param idRangeEnd the first unallocated entry for idType
     */
    public void firstUnallocated( IdType idType, long idRangeEnd )
    {
        firstUnallocated[idType.ordinal()] = idRangeEnd;
    }

    /**
     * @param idType The type of graph object whose ID is under allocation
     * @return start position of allocation
     */
    public long lastIdRangeStart( IdType idType )
    {
        return lastIdRangeStartForMe[idType.ordinal()];
    }

    /**
     * @param idType       The type of graph object whose ID is under allocation
     * @param idRangeStart start position of allocation
     */
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

        IdAllocationState that = (IdAllocationState) o;

        return logIndex == that.logIndex &&
                Arrays.equals( firstUnallocated, that.firstUnallocated ) &&
                Arrays.equals( lastIdRangeStartForMe, that.lastIdRangeStartForMe ) &&
                Arrays.equals( lastIdRangeLengthForMe, that.lastIdRangeLengthForMe );
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

    public static class Marshal implements StateMarshal<IdAllocationState>
    {
        @Override
        public void marshal( IdAllocationState state, WritableChannel channel ) throws IOException
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
        public IdAllocationState unmarshal( ReadableChannel channel ) throws IOException
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

                return new IdAllocationState( firstNotAllocated, lastIdRangeStartForMe,
                        lastIdRangeLengthForMe, logIndex );
            }
            catch ( ReadPastEndException ex )
            {
                return null;
            }
        }

        @Override
        public IdAllocationState startState()
        {
            return new IdAllocationState();
        }

        @Override
        public long ordinal( IdAllocationState state )
        {
            return state.logIndex();
        }
    }
}
