/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.membership;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

public class RaftTestGroup implements RaftGroup<MemberId>
{
    private final Set<MemberId> members = new HashSet<>();

    public RaftTestGroup( Set<MemberId> members )
    {
        this.members.addAll( members );
    }

    public RaftTestGroup( int... memberIds )
    {
        for ( int memberId : memberIds )
        {
            this.members.add( member( memberId ) );
        }
    }

    public RaftTestGroup( MemberId... memberIds )
    {
        this.members.addAll( Arrays.asList( memberIds ) );
    }

    @Override
    public Set<MemberId> getMembers()
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
