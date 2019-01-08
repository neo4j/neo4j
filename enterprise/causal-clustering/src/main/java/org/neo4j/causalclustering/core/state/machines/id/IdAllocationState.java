/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.id;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
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
