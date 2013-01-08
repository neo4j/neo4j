/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.Serializable;
import java.net.URI;
import java.util.Set;

import org.neo4j.cluster.com.message.MessageType;

/**
 * Messages used by the {@link HeartbeatState} state machine.
 */
public enum HeartbeatMessage
        implements MessageType
{
    // Heartbeat API messages
    addHeartbeatListener, removeHeartbeatListener,

    // Protocol messages
    join, leave,
    i_am_alive, timed_out, sendHeartbeat, reset_send_heartbeat, suspicions;

    public static class IAmAliveState
            implements Serializable
    {
        private URI server;

        public IAmAliveState( URI server )
        {
            this.server = server;
        }

        public URI getServer()
        {
            return server;
        }
    }

    public static class SuspicionsState
            implements Serializable
    {
        private Set<URI> suspicions;

        public SuspicionsState( Set<URI> suspicions )
        {
            this.suspicions = suspicions;
        }

        public Set<URI> getSuspicions()
        {
            return suspicions;
        }
    }
}
