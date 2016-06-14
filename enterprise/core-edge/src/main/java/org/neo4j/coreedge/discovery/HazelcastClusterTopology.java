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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.BOLT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.RAFT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.TRANSACTION_SERVER;

public class HazelcastClusterTopology
{
    static final String EDGE_SERVERS = "edge-servers";

    static ClusterTopology fromHazelcastInstance( HazelcastInstance hazelcastInstance )
    {
        Set<Member> coreMembers = hazelcastInstance.getCluster().getMembers();
        return new ClusterTopology( canBeBootstrapped( coreMembers ), toCoreMemberMap( coreMembers ),
                edgeMembers( hazelcastInstance ) );
    }

    private static Set<BoltAddress> edgeMembers( HazelcastInstance hazelcastInstance )
    {
        if ( hazelcastInstance == null )
        {
            return emptySet();
        }

        return hazelcastInstance.<String>getSet( EDGE_SERVERS ).stream()
                .map( hostnamePort -> new BoltAddress( new AdvertisedSocketAddress( hostnamePort ) ) )
                .collect( toSet() );
    }

    private static boolean canBeBootstrapped( Set<Member> coreMembers )
    {
        Member firstMember = coreMembers.iterator().next();
        return firstMember.localMember();
    }

    private static Map<CoreMember, BoltAddress> toCoreMemberMap( Set<Member> members )
    {
        Map<CoreMember, BoltAddress> coreMembers = new HashMap<>();

        for ( Member member : members )
        {
            coreMembers.put( new CoreMember(
                    new AdvertisedSocketAddress( member.getStringAttribute( TRANSACTION_SERVER ) ),
                    new AdvertisedSocketAddress( member.getStringAttribute( RAFT_SERVER ) )),
                    new BoltAddress( new AdvertisedSocketAddress( member.getStringAttribute( BOLT_SERVER ) ) )
            );
        }

        return coreMembers;
    }
}
