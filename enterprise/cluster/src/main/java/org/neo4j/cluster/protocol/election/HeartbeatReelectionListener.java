/*
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

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * If an instance is considered failed, demote it from all its roles in the cluster.
 * If an instance comes back, ensure that all roles are elected.
 */
public class HeartbeatReelectionListener
    implements HeartbeatListener
{
    private final Election election;
    private final StringLogger messagesLog;

    public HeartbeatReelectionListener( Election election, StringLogger messagesLog )
    {
        this.election = election;
        this.messagesLog = messagesLog;
    }

    @Override
    public void failed( InstanceId server )
    {
        // Suggest reelection for all roles of this node
        messagesLog.warn( " instance " + server +" is being demoted since it failed" );
        election.demote( server );
    }

    @Override
    public void alive( InstanceId server )
    {
        election.performRoleElections();
    }
}
