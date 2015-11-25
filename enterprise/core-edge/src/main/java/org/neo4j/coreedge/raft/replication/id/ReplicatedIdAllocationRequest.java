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

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.kernel.IdType;

import static java.lang.String.format;

/**
 * This type is handled by the ReplicatedIdAllocationStateMachine. */
public class ReplicatedIdAllocationRequest implements ReplicatedContent
{
    private final CoreMember owner;
    private final IdType idType;
    private final long idRangeStart;
    private final int idRangeLength;

    public ReplicatedIdAllocationRequest( CoreMember owner, IdType idType, long idRangeStart, int idRangeLength )
    {
        this.owner = owner;
        this.idType = idType;
        this.idRangeStart = idRangeStart;
        this.idRangeLength = idRangeLength;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        ReplicatedIdAllocationRequest that = (ReplicatedIdAllocationRequest) o;

        if ( idRangeStart != that.idRangeStart )
        { return false; }
        if ( idRangeLength != that.idRangeLength )
        { return false; }
        if ( !owner.equals( that.owner ) )
        { return false; }
        return idType == that.idType;

    }

    @Override
    public int hashCode()
    {
        int result = owner.hashCode();
        result = 31 * result + idType.hashCode();
        result = 31 * result + (int) (idRangeStart ^ (idRangeStart >>> 32));
        result = 31 * result + idRangeLength;
        return result;
    }

    public CoreMember owner()
    {
        return owner;
    }

    public IdType idType()
    {
        return idType;
    }

    public long idRangeStart()
    {
        return idRangeStart;
    }

    public int idRangeLength()
    {
        return idRangeLength;
    }

    @Override
    public String toString()
    {
        return format( "ReplicatedIdAllocationRequest{owner=%s, idType=%s, idRangeStart=%d, idRangeLength=%d}", owner, idType, idRangeStart, idRangeLength );
    }
}
