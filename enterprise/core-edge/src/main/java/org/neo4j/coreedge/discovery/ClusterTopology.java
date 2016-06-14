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
import java.util.Map;
import java.util.Set;

import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;

public class ClusterTopology
{
    private final Map<CoreMember, BoltAddress> coreMembers;
    private final Set<BoltAddress> edgeBoltAddresses;
    private final boolean canBeBootstrapped;

    public ClusterTopology( boolean canBeBootstrapped,
                            Map<CoreMember, BoltAddress> coreMembers,
                            Set<BoltAddress> edgeBoltAddresses )
    {
        this.canBeBootstrapped = canBeBootstrapped;
        this.edgeBoltAddresses = edgeBoltAddresses;
        this.coreMembers = new HashMap<>( coreMembers );
    }

    public Set<CoreMember> coreMembers()
    {
        return coreMembers.keySet();
    }

    public Set<BoltAddress> edgeMembers()
    {
        return edgeBoltAddresses;
    }

    boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
    }

    public BoltAddress boltAddress(CoreMember coreMember)
    {
         return coreMembers.get( coreMember );
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
