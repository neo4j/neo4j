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

import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.DISCOVERY_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.RAFT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.TRANSACTION_SERVER;
import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class HazelcastClusterTopology implements ClusterTopology
{
    public static final String EDGE_SERVERS = "edge-servers";

    private final boolean bootstrappable;
    private final Set<CoreMember> coreMembers;
    private final int edgeServerCount;

    public HazelcastClusterTopology( HazelcastInstance hazelcast )
    {
        Set<Member> hazelcastMembers = hazelcast.getCluster().getMembers();
        this.bootstrappable = hazelcastMembers.iterator().next().localMember();

        this.coreMembers = new HashSet<>();
        for ( Member member : hazelcastMembers )
        {
            coreMembers.add( new CoreMember(
                    address( member.getStringAttribute( DISCOVERY_SERVER ) ),
                    address( member.getStringAttribute( TRANSACTION_SERVER ) ),
                    address( member.getStringAttribute( RAFT_SERVER ) )
            ));
        }

        this.edgeServerCount = hazelcast.getMap( EDGE_SERVERS ).size();
    }

    @Override
    public boolean bootstrappable()
    {
        return bootstrappable;
    }

    @Override
    public int getNumberOfCoreServers()
    {
        return coreMembers.size();
    }

    @Override
    public Set<CoreMember> getMembers()
    {
        return coreMembers;
    }

    @Override
    public int getNumberOfEdgeServers()
    {
        return edgeServerCount;
    }

    @Override
    public AdvertisedSocketAddress firstTransactionServer()
    {
        CoreMember member = coreMembers.iterator().next();
        return member.getCoreAddress();
    }
}
