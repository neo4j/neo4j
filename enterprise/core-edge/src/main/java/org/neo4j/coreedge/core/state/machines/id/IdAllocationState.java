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
package org.neo4j.coreedge.core.state.machines.id;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.coreedge.core.state.storage.SafeStateMarshal;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

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
 */
public class IdAllocationState implements UnallocatedIds
{
    private final long[] firstUnallocated;
    private long logIndex;

    IdAllocationState()
    {
        this( new long[IdType.values().length], -1L );
    }

    public IdAllocationState( long[] firstUnallocated, long logIndex )
    {
        this.firstUnallocated = firstUnallocated;
        this.logIndex = logIndex;
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
    @Override
    public long firstUnallocated( IdType idType )
    {
        return firstUnallocated[idType.ordinal()];
    }

    /**
     * @param idType     the type of graph object whose ID is under allocation
     * @param idRangeEnd the first unallocated entry for idType
     */
    void firstUnallocated( IdType idType, long idRangeEnd )
    {
        firstUnallocated[idType.ordinal()] = idRangeEnd;
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
                Arrays.equals( firstUnallocated, that.firstUnallocated );
    }

    @Override
    public int hashCode()
    {
        int result = Arrays.hashCode( firstUnallocated );
        result = 31 * result + (int) (logIndex ^ (logIndex >>> 32));
        return result;
    }

    public IdAllocationState newInstance()
    {
        return new IdAllocationState( firstUnallocated.clone(), logIndex );
    }

    public static class Marshal extends SafeStateMarshal<IdAllocationState>
    {
        @Override
        public void marshal( IdAllocationState state, WritableChannel channel ) throws IOException
        {
            channel.putLong( (long) state.firstUnallocated.length );
            for ( long l : state.firstUnallocated )
            {
                channel.putLong( l );
            }

            channel.putLong( state.logIndex );
        }

        @Override
        public IdAllocationState unmarshal0( ReadableChannel channel ) throws IOException
        {
            long[] firstNotAllocated = new long[(int) channel.getLong()];

            for ( int i = 0; i < firstNotAllocated.length; i++ )
            {
                firstNotAllocated[i] = channel.getLong();
            }

            long logIndex = channel.getLong();

            return new IdAllocationState( firstNotAllocated, logIndex );
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

    @Override
    public String toString()
    {
        return String.format( "IdAllocationState{firstUnallocated=%s, logIndex=%d}",
                Arrays.toString( firstUnallocated ), logIndex );
    }
}
