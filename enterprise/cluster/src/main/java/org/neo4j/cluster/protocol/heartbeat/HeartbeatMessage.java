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
package org.neo4j.cluster.protocol.heartbeat;

import java.io.Serializable;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
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
        private static final long serialVersionUID = 6799806932628197123L;

        private InstanceId server;

        public IAmAliveState( InstanceId server )
        {
            this.server = server;
        }

        public InstanceId getServer()
        {
            return server;
        }

        @Override
        public String toString()
        {
            return "i_am_alive[" + server + "]";
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            IAmAliveState that = (IAmAliveState) o;

            return server != null ? server.equals( that.server ) : that.server == null;
        }

        @Override
        public int hashCode()
        {
            return server != null ? server.hashCode() : 0;
        }
    }

    public static class SuspicionsState
            implements Serializable
    {

        private static final long serialVersionUID = 3152836192116904427L;

        private Set<InstanceId> suspicions;

        public SuspicionsState( Set<InstanceId> suspicions )
        {
            this.suspicions = suspicions;
        }

        public Set<InstanceId> getSuspicions()
        {
            return suspicions;
        }

        @Override
        public String toString()
        {
            return "Suspicions:" + suspicions;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            SuspicionsState that = (SuspicionsState) o;

            return suspicions != null ? suspicions.equals( that.suspicions ) : that.suspicions == null;
        }

        @Override
        public int hashCode()
        {
            return suspicions != null ? suspicions.hashCode() : 0;
        }
    }
}
