/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.election;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * When an instance leaves a cluster, demote it from all its current roles.
 */
public class ClusterLeaveReelectionListener
        extends ClusterListener.Adapter
{
    private final Election election;
    private final StringLogger logger;

    public ClusterLeaveReelectionListener( Election election, StringLogger logger )
    {
        this.election = election;
        this.logger = logger;
    }

    @Override
    public void leftCluster( InstanceId instanceId, URI member )
    {
        String name = instanceId.instanceNameFromURI( member );
        logger.warn( "Demoting member " + name + " because it left the cluster" );
        // Suggest reelection for all roles of this node
        election.demote( instanceId );
    }
}
