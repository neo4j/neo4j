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
package org.neo4j.causalclustering.discovery;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;

public class CoreTopology
{
    public static CoreTopology EMPTY = new CoreTopology( null, false, Collections.emptyMap() );

    private final ClusterId clusterId;
    private final boolean canBeBootstrapped;
    private final Map<MemberId, CoreAddresses> coreMembers;

    public CoreTopology( ClusterId clusterId, boolean canBeBootstrapped, Map<MemberId, CoreAddresses> coreMembers )
    {
        this.clusterId = clusterId;
        this.canBeBootstrapped = canBeBootstrapped;
        this.coreMembers = new HashMap<>( coreMembers );
    }

    public Set<MemberId> members()
    {
        return coreMembers.keySet();
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public Collection<CoreAddresses> addresses()
    {
        return coreMembers.values();
    }

    public boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
    }

    public Optional<CoreAddresses> find( MemberId memberId )
    {
        return Optional.ofNullable( coreMembers.get( memberId ) );
    }

    @Override
    public String toString()
    {
        return String.format( "{coreMembers=%s, bootstrappable=%s}", coreMembers, canBeBootstrapped() );
    }

}
