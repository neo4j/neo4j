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
        if ( heartbeatContext.isFailedBasedOnSuspicions( instanceId ) )
        {
            log.warn( "Instance " + instanceId + " (" + member + ") has left the cluster " +
                    "but is still treated as failed by HeartbeatContext" );

            heartbeatContext.serverLeftCluster( instanceId );
        }
    }
}
