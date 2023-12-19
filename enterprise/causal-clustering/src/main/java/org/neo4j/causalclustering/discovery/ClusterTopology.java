/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Optional;

import org.neo4j.causalclustering.identity.MemberId;

public class ClusterTopology
{
    private final CoreTopology coreTopology;
    private final ReadReplicaTopology readReplicaTopology;

    public ClusterTopology( CoreTopology coreTopology, ReadReplicaTopology readReplicaTopology )
    {
        this.coreTopology = coreTopology;
        this.readReplicaTopology = readReplicaTopology;
    }

    public Optional<CatchupServerAddress> find( MemberId upstream )
    {
        Optional<CatchupServerAddress> coreCatchupAddress = coreTopology.find( upstream ).map( a -> (CatchupServerAddress) a );
        Optional<CatchupServerAddress> readCatchupAddress = readReplicaTopology.find( upstream ).map( a -> (CatchupServerAddress) a );

        return coreCatchupAddress.map( Optional::of ).orElse( readCatchupAddress );
    }
}
