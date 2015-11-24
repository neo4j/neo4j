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
package org.neo4j.coreedge.raft.membership;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.server.RaftTestMember;

import static java.lang.String.format;

public class RaftTestGroup implements RaftGroup<RaftTestMember>
{
    private final Set<RaftTestMember> members = new HashSet<>();

    public RaftTestGroup( Set<RaftTestMember> members )
    {
        this.members.addAll( members );
    }

    public RaftTestGroup( long... memberIds )
    {
        for ( long memberId : memberIds )
        {
            this.members.add( RaftTestMember.member( memberId ) );
        }
    }

    @Override
    public Set<RaftTestMember> getMembers()
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

        RaftTestGroup that = (RaftTestGroup) o;

        return members.equals( that.members );

    }

    @Override
    public int hashCode()
    {
        return members.hashCode();
    }

    @Override
    public String toString()
    {
        return format( "RaftTestGroup{members=%s}", members );
    }
}
