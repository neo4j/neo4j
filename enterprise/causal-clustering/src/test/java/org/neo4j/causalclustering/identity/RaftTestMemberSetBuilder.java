/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
