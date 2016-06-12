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

import java.util.ArrayList;
import java.util.Set;

import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.helpers.collection.Iterables;

public class ClusterTopology
{
    private final ArrayList<CoreMember> coreMembers;
    private final Set<BoltAddress> boltAddresses;
    private final Set<BoltAddress> edgeBoltAddresses;
    private final boolean canBeBootstrapped;

    ClusterTopology( boolean canBeBootstrapped, Set<CoreMember> coreMembers,
                     Set<BoltAddress> coreBoltAddresses, Set<BoltAddress> edgeBoltAddresses )
    {
        this.canBeBootstrapped = canBeBootstrapped;
        this.boltAddresses = coreBoltAddresses;
        this.edgeBoltAddresses = edgeBoltAddresses;
        this.coreMembers = new ArrayList<>( coreMembers );
    }

    public Set<CoreMember> coreMembers()
    {
        return Iterables.asSet( coreMembers );
    }

    public Set<BoltAddress> edgeMembers()
    {
        return edgeBoltAddresses;
    }

    public boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
    }

    public Set<BoltAddress> boltCoreMembers()
    {
        return boltAddresses;
    }

    @Override
    public String toString()
    {
        return "TestOnlyClusterTopology{" +
                "coreMembers.size()=" + coreMembers.size() +
                ", bootstrappable=" + canBeBootstrapped() +
                ", edgeMembers.size()=" + edgeBoltAddresses.size() +
                '}';
    }
}
