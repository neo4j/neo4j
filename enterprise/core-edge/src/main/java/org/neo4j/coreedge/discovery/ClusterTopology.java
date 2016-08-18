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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;

public class ClusterTopology
{
    private ClusterId clusterId;
    private final Map<MemberId, CoreAddresses> coreMembers;
    private final Set<EdgeAddresses> edgeAddresses;
    private final boolean canBeBootstrapped;

    public ClusterTopology( ClusterId clusterId, boolean canBeBootstrapped,
            Map<MemberId,CoreAddresses> coreMembers,
            Set<EdgeAddresses> edgeAddresses )
    {
        this.clusterId = clusterId;
        this.canBeBootstrapped = canBeBootstrapped;
        this.edgeAddresses = edgeAddresses;
        this.coreMembers = new HashMap<>( coreMembers );
    }

    public Set<MemberId> coreMembers()
    {
        return coreMembers.keySet();
    }

    public Collection<CoreAddresses> coreMemberAddresses()
    {
        return coreMembers.values();
    }

    public Collection<EdgeAddresses> edgeMemberAddresses()
    {
        return edgeAddresses;
    }

    public boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
    }

    public CoreAddresses coreAddresses( MemberId memberId ) throws NoKnownAddressesException
    {
        CoreAddresses coreAddresses = coreMembers.get( memberId );
        if ( coreAddresses == null )
        {
            throw new NoKnownAddressesException();
        }
        return coreAddresses;
    }

    @Override
    public String toString()
    {
        return String.format( "{coreMembers=%s, bootstrappable=%s, edgeMemberAddresses=%s}",
                coreMembers.keySet(), canBeBootstrapped(), edgeAddresses );
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }
}
