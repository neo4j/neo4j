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
package org.neo4j.coreedge.discovery;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.core.Member;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;

import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.BOLT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.RAFT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.TRANSACTION_SERVER;

public class HazelcastClusterTopology implements ClusterTopology
{
    static final String EDGE_SERVERS = "edge-servers";
    private final Set<Member> coreMembers;
    private Set<BoltAddress> edgeMembers;

    HazelcastClusterTopology( Set<Member> coreMembers, Set<BoltAddress> edgeMembers )
    {
        this.coreMembers = coreMembers;
        this.edgeMembers = edgeMembers;
    }

    @Override
    public boolean bootstrappable()
    {
        Member firstMember = coreMembers.iterator().next();
        return firstMember.localMember();
    }

    @Override
    public Set<BoltAddress> boltCoreMembers()
    {
        return toBoltMembers( coreMembers );
    }

    private Set<BoltAddress> toBoltMembers( Set<Member> members )
    {
        Set<BoltAddress> coreMembers = new HashSet<>();

        for ( Member member : members )
        {
            if ( member.getStringAttribute( BOLT_SERVER ) != null )
            {
                coreMembers.add( new BoltAddress(
                        new AdvertisedSocketAddress( member.getStringAttribute( BOLT_SERVER ) ) ) );
            }
        }

        return coreMembers;
    }

    @Override
    public Set<CoreMember> coreMembers()
    {
        return toCoreMembers( coreMembers );
    }

    @Override
    public Set<BoltAddress> edgeMembers()
    {
        return edgeMembers;
    }

    private Set<CoreMember> toCoreMembers( Set<Member> members )
    {
        HashSet<CoreMember> coreMembers = new HashSet<>();

        for ( Member member : members )
        {
            coreMembers.add( new CoreMember(
                    new AdvertisedSocketAddress( member.getStringAttribute( TRANSACTION_SERVER ) ),
                    new AdvertisedSocketAddress( member.getStringAttribute( RAFT_SERVER ) ),
                    new AdvertisedSocketAddress( member.getStringAttribute( BOLT_SERVER ) )
            ) );
        }

        return coreMembers;
    }
}
