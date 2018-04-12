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
package org.neo4j.causalclustering.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;

public class CoreTopology implements Topology<CoreServerInfo>
{
    static CoreTopology EMPTY = new CoreTopology( null, false, emptyMap() );

    private final ClusterId clusterId;
    private final boolean canBeBootstrapped;
    private final Map<MemberId,CoreServerInfo> coreMembers;

    public CoreTopology( ClusterId clusterId, boolean canBeBootstrapped, Map<MemberId,CoreServerInfo> coreMembers )
    {
        this.clusterId = clusterId;
        this.canBeBootstrapped = canBeBootstrapped;
        this.coreMembers = new HashMap<>( coreMembers );
    }

    @Override
    public Map<MemberId,CoreServerInfo> members()
    {
        return coreMembers;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
    }

    @Override
    public String toString()
    {
        return format( "{clusterId=%s, bootstrappable=%s, coreMembers=%s}", clusterId, canBeBootstrapped(), coreMembers );
    }

    public Optional<MemberId> randomCoreMemberId()
    {
        if ( coreMembers.isEmpty() )
        {
            return Optional.empty();
        }
        return coreMembers.keySet().stream().skip( ThreadLocalRandom.current().nextInt( coreMembers.size() ) ).findFirst();
    }

    @Override
    public CoreTopology filterTopologyByDb( String dbName )
    {
        Map<MemberId, CoreServerInfo> filteredMembers = filterHostsByDb( members(), dbName );

        return new CoreTopology( clusterId(), canBeBootstrapped(), filteredMembers );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        CoreTopology that = (CoreTopology) o;
        return Objects.equals( coreMembers, that.coreMembers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( coreMembers );
    }
}
