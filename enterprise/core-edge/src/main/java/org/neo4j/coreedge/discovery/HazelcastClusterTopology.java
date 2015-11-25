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
package org.neo4j.coreedge.discovery;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.TRANSACTION_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.RAFT_SERVER;

public class HazelcastClusterTopology implements ClusterTopology
{
    public static final String EDGE_SERVERS = "edge-servers";
    private HazelcastInstance hazelcast;

    public HazelcastClusterTopology( HazelcastInstance hazelcast )
    {
        this.hazelcast = hazelcast;
    }

    @Override
    public boolean bootstrappable()
    {
        Member firstMember = hazelcast.getCluster().getMembers().iterator().next();
        return firstMember.localMember();
    }

    @Override
    public int getNumberOfCoreServers()
    {
        return hazelcast.getCluster().getMembers().size();
    }

    @Override
    public Set<CoreMember> getMembers()
    {
        return toCoreMembers( hazelcast.getCluster().getMembers() );
    }

    private Set<CoreMember> toCoreMembers( Set<Member> members )
    {
        HashSet<CoreMember> coreMembers = new HashSet<>();

        for ( Member member : members )
        {
            coreMembers.add( new CoreMember(
                    address( member.getStringAttribute( TRANSACTION_SERVER ) ),
                    address( member.getStringAttribute( RAFT_SERVER ) )
            ));
        }

        return coreMembers;
    }

    @Override
    public int getNumberOfEdgeServers()
    {
        return hazelcast.getMap( EDGE_SERVERS ).size();
    }

    @Override
    public AdvertisedSocketAddress firstTransactionServer()
    {
        Member member = hazelcast.getCluster().getMembers().iterator().next();
        return address( member.getStringAttribute( TRANSACTION_SERVER ) );
    }
}
