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
package org.neo4j.coreedge.raft.replication.id;

import org.neo4j.kernel.IdType;

public class InMemoryIdAllocationStateStore implements IdAllocationState
{
    private final long[] firstNotAllocated = new long[IdType.values().length];
    private final long[] lastIdRangeStartForMe = new long[IdType.values().length];
    private final int[] lastIdRangeLengthForMe = new int[IdType.values().length];

    @Override
    public int lastIdRangeLengthForMe( IdType idType )
    {
        return lastIdRangeLengthForMe[idType.ordinal()];
    }

    @Override
    public void lastIdRangeLengthForMe( IdType idType, int idRangeLength )
    {
        lastIdRangeLengthForMe[idType.ordinal()] = idRangeLength;
    }

    @Override
    public long firstNotAllocated( IdType idType )
    {
        return firstNotAllocated[idType.ordinal()];
    }

    @Override
    public void firstNotAllocated( IdType idType, long idRangeEnd )
    {
        firstNotAllocated[idType.ordinal()] = idRangeEnd;
    }

    @Override
    public long lastIdRangeStartForMe( IdType idType )
    {
        return lastIdRangeStartForMe[idType.ordinal()];
    }

    @Override
    public void lastIdRangeStartForMe( IdType idType, long idRangeStart )
    {
        lastIdRangeStartForMe[idType.ordinal()] = idRangeStart;
    }
}
