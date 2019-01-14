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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.causalclustering.identity.MemberId;

import static java.util.Collections.emptyMap;

public class ReadReplicaTopology implements Topology<ReadReplicaInfo>
{
    static final ReadReplicaTopology EMPTY = new ReadReplicaTopology( emptyMap() );

    private final Map<MemberId,ReadReplicaInfo> readReplicaMembers;

    public ReadReplicaTopology( Map<MemberId,ReadReplicaInfo> readReplicaMembers )
    {
        this.readReplicaMembers = readReplicaMembers;
    }

    public Collection<ReadReplicaInfo> allMemberInfo()
    {
        return readReplicaMembers.values();
    }

    @Override
    public Map<MemberId, ReadReplicaInfo> members()
    {
        return readReplicaMembers;
    }

    @Override
    public String toString()
    {
        return String.format( "{readReplicas=%s}", readReplicaMembers );
    }

    @Override
    public ReadReplicaTopology filterTopologyByDb( String dbName )
    {
        Map<MemberId, ReadReplicaInfo> filteredMembers = filterHostsByDb( members(), dbName );

        return new ReadReplicaTopology( filteredMembers );
    }
}
