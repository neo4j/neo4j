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
package org.neo4j.cluster.protocol.election;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * When an instance leaves a cluster, demote it from all its current roles.
 */
public class ClusterLeaveReelectionListener
        extends ClusterListener.Adapter
{
    private final Election election;
    private final Log log;

    public ClusterLeaveReelectionListener( Election election, LogProvider logProvider )
    {
        this.election = election;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void leftCluster( InstanceId instanceId, URI member )
    {
        String name = instanceId.instanceNameFromURI( member );
        log.warn( "Demoting member " + name + " because it left the cluster" );
        // Suggest reelection for all roles of this node
        election.demote( instanceId );
    }
}
