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
package org.neo4j.coreedge.server;

import java.io.IOException;

import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class RaftTestMember implements Comparable<RaftTestMember>
{
    public static RaftTestMember member( long id )
    {
        return new RaftTestMember( id );
    }

    private final long id;

    public RaftTestMember( long id )
    {
        this.id = id;
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

        RaftTestMember member = (RaftTestMember) o;

        return id == member.id;

    }

    public long getId()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString()
    {
        return String.format( "M{id=%d}", id );
    }

    @Override
    public int compareTo( RaftTestMember other )
    {
        return new Long( this.id ).compareTo( other.getId() );
    }

    public static class RaftTestMemberMarshal implements ChannelMarshal<RaftTestMember>
    {

        @Override
        public void marshal( RaftTestMember target, WritableChannel channel ) throws IOException
        {
            channel.putLong( target.id );
        }

        @Override
        public RaftTestMember unmarshal( ReadableChannel source ) throws IOException
        {
            try
            {
                return member( source.getLong() );
            }
            catch ( ReadPastEndException notEnoughBytes )
            {
                return null;
            }
        }
    }
}
