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
package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class HeartbeatLeftListener extends ClusterListener.Adapter
{
    private final HeartbeatContext heartbeatContext;
    private final Log log;

    public HeartbeatLeftListener( HeartbeatContext heartbeatContext, LogProvider logProvider )
    {
        this.heartbeatContext = heartbeatContext;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void leftCluster( InstanceId instanceId, URI member )
    {
        if ( heartbeatContext.isFailed( instanceId ) )
        {
            log.warn( "Instance " + instanceId + " (" + member + ") has left the cluster " +
                    "but is still treated as failed by HeartbeatContext" );

            heartbeatContext.serverLeftCluster( instanceId );
        }
    }
}
