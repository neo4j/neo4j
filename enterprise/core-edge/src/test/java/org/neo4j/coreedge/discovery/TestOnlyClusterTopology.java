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

import java.util.ArrayList;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class TestOnlyClusterTopology implements ClusterTopology
{
    private final ArrayList<CoreMember> coreMembers;
    private final Set<InstanceId> edgeMembers;
    private final boolean canBeBootstrapped;

    public TestOnlyClusterTopology( boolean canBeBootstrapped, Set<CoreMember> coreMembers, Set<InstanceId> edgeMembers )
    {
        this.canBeBootstrapped = canBeBootstrapped;
        this.edgeMembers = edgeMembers;
        this.coreMembers = new ArrayList<>( coreMembers );
    }

    @Override
    public AdvertisedSocketAddress firstTransactionServer()
    {
        return coreMembers.size() > 0 ? coreMembers.get( 0 ).getCoreAddress() : null;
    }

    @Override
    public int getNumberOfEdgeServers()
    {
        return edgeMembers.size();
    }

    @Override
    public int getNumberOfCoreServers()
    {
        return coreMembers.size();
    }

    @Override
    public Set<CoreMember> getMembers()
    {
        return asSet( coreMembers );
    }

    @Override
    public boolean bootstrappable()
    {
        // Can only bootstrap for a cluster with multiple instances
        return canBeBootstrapped && coreMembers.size() > 1;
    }

    @Override
    public String toString()
    {
        return "TestOnlyClusterTopology{" +
                "coreMembers.size()=" + coreMembers.size() +
                ", bootstrappable=" + bootstrappable() +
                ", edgeMembers.size()=" + edgeMembers.size() +
                '}';
    }
}
