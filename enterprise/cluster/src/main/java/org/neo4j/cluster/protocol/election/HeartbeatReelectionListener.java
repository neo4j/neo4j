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
package org.neo4j.cluster.protocol.election;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * If an instance is considered failed, demote it from all its roles in the cluster.
 * If an instance comes back, ensure that all roles are elected.
 */
public class HeartbeatReelectionListener
    implements HeartbeatListener
{
    private final Election election;
    private final Log log;

    public HeartbeatReelectionListener( Election election, LogProvider logProvider )
    {
        this.election = election;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void failed( InstanceId server )
    {
        // Suggest reelection for all roles of this node
        log.warn( " instance " + server + " is being demoted since it failed" );
        election.demote( server );
    }

    @Override
    public void alive( InstanceId server )
    {
        election.performRoleElections();
    }
}
