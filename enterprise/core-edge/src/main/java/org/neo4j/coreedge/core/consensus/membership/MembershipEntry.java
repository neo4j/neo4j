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
package org.neo4j.coreedge.core.consensus.membership;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.coreedge.messaging.EndOfStreamException;
import org.neo4j.coreedge.core.state.storage.SafeStateMarshal;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * Represents a membership entry in the RAFT log.
 */
class MembershipEntry
{
    private long logIndex;
    private Set<MemberId> members;

    MembershipEntry( long logIndex, Set<MemberId> members )
    {
        this.members = members;
        this.logIndex = logIndex;
    }

    public long logIndex()
    {
        return logIndex;
    }

    public Set<MemberId> members()
    {
        return members;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        MembershipEntry that = (MembershipEntry) o;
        return logIndex == that.logIndex &&
               Objects.equals( members, that.members );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( logIndex, members );
    }

    @Override
    public String toString()
    {
        return "MembershipEntry{" +
               "logIndex=" + logIndex +
               ", members=" + members +
               '}';
    }

    static class Marshal extends SafeStateMarshal<MembershipEntry>
    {
        MemberId.Marshal memberMarshal = new MemberId.Marshal();

        @Override
        public MembershipEntry startState()
        {
            return null;
        }

        @Override
        public long ordinal( MembershipEntry entry )
        {
            return entry.logIndex;
        }

        @Override
        public void marshal( MembershipEntry entry, WritableChannel channel ) throws IOException
        {
            if ( entry == null )
            {
                channel.putInt( 0 );
                return;
            }
            else
            {
                channel.putInt( 1 );
            }

            channel.putLong( entry.logIndex );
            channel.putInt( entry.members.size() );
            for ( MemberId member : entry.members )
            {
                memberMarshal.marshal( member, channel );
            }
        }

        @Override
        public MembershipEntry unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            int hasEntry = channel.getInt();
            if ( hasEntry == 0 )
            {
                return null;
            }
            long logIndex = channel.getLong();
            int memberCount = channel.getInt();
            Set<MemberId> members = new HashSet<>();
            for ( int i = 0; i < memberCount; i++ )
            {
                members.add( memberMarshal.unmarshal( channel ) );
            }
            return new MembershipEntry( logIndex, members );
        }
    }
}
