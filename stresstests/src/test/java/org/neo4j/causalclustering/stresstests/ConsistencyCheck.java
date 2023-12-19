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
package org.neo4j.causalclustering.stresstests;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.collection.Iterables;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;

/**
 * Check the consistency of all the cluster members' stores.
 */
public class ConsistencyCheck extends Validation
{
    private final Cluster cluster;

    ConsistencyCheck( Resources resources )
    {
        super();
        cluster = resources.cluster();
    }

    @Override
    protected void validate() throws Exception
    {
        Iterable<ClusterMember> members = Iterables.concat( cluster.coreMembers(), cluster.readReplicas() );

        for ( ClusterMember member : members )
        {
            String storeDir = member.storeDir().getAbsolutePath();
            ConsistencyCheckService.Result result = runConsistencyCheckTool( new String[]{storeDir}, System.out, System.err );
            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + storeDir );
            }
        }
    }
}
