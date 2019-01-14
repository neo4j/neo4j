/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
    static final CoreTopology EMPTY = new CoreTopology( null, false, emptyMap() );

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
