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
package org.neo4j.causalclustering.core.consensus.membership;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * Represents a membership entry in the RAFT log.
 */
public class MembershipEntry
{
    private long logIndex;
    private Set<MemberId> members;

    public MembershipEntry( long logIndex, Set<MemberId> members )
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
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MembershipEntry that = (MembershipEntry) o;
        return logIndex == that.logIndex && Objects.equals( members, that.members );
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

    public static class Marshal extends SafeStateMarshal<MembershipEntry>
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
        protected MembershipEntry unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
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
