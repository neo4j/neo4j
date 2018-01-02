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
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterListener;

/**
 * When an instance joins a cluster, setup a heartbeat for it
 */
public class HeartbeatJoinListener extends ClusterListener.Adapter
{
    private final MessageHolder outgoing;

    public HeartbeatJoinListener( MessageHolder outgoing )
    {
        this.outgoing = outgoing;
    }

    @Override
    public void joinedCluster( InstanceId member, URI atUri )
    {
        outgoing.offer( Message.internal( HeartbeatMessage.reset_send_heartbeat, member ) );
    }
}
