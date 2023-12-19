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
package org.neo4j.causalclustering.identity;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.membership.RaftGroup;
import org.neo4j.causalclustering.core.consensus.membership.RaftTestGroup;

import static org.neo4j.causalclustering.identity.RaftTestMember.member;

public class RaftTestMemberSetBuilder implements RaftGroup.Builder
{
    public static RaftTestMemberSetBuilder INSTANCE = new RaftTestMemberSetBuilder();

    private RaftTestMemberSetBuilder()
    {
    }

    @Override
    public RaftGroup build( Set members )
    {
        return new RaftTestGroup( members );
    }

    public static RaftGroup memberSet( int... ids )
    {
        HashSet members = new HashSet<>();
        for ( int id : ids )
        {
            members.add( member( id ) );
        }
        return new RaftTestGroup( members );
    }
}
