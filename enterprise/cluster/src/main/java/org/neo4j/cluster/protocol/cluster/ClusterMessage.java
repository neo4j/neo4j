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
package org.neo4j.cluster.protocol.cluster;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Messages to implement the Cluster API state machine
 *
 * @see Cluster
 * @see ClusterState
 */
public enum ClusterMessage
        implements MessageType
{
    // Method messages
    create, createResponse, join, joinResponse, leave, leaveResponse,

    addClusterListener, removeClusterListener,

    // Protocol messages
    joining, joiningTimeout,
    configurationRequest, configurationResponse, configurationTimeout, configurationChanged,
    joinDenied, joinFailure, leaveTimedout;

    public static class ConfigurationRequestState implements Serializable, Comparable<ConfigurationRequestState>
    {
        private InstanceId joiningId;
        private URI joiningUri;

        public ConfigurationRequestState( InstanceId joiningId, URI joiningUri )
        {
            this.joiningId = joiningId;
            this.joiningUri = joiningUri;
        }

        public InstanceId getJoiningId()
        {
            return joiningId;
        }

        public URI getJoiningUri()
        {
            return joiningUri;
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

            ConfigurationRequestState that = (ConfigurationRequestState) o;

            if ( !joiningId.equals( that.joiningId ) )
            {
                return false;
            }
            if ( !joiningUri.equals( that.joiningUri ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = joiningId.hashCode();
            result = 31 * result + joiningUri.hashCode();
            return result;
        }

        @Override
        public int compareTo( ConfigurationRequestState o )
        {
            return this.joiningId.compareTo( o.joiningId );
        }

        @Override
        public String toString()
        {
            return joiningId + ":" + joiningUri;
        }
    }

    public static class ConfigurationResponseState
            implements Serializable
    {
        private Map<InstanceId, URI> nodes;
        private org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId latestReceivedInstanceId;
        private Map<String, InstanceId> roles;
        private String clusterName;

        public ConfigurationResponseState( Map<String, InstanceId> roles, Map<InstanceId, URI> nodes, org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId latestReceivedInstanceId,
                                          String clusterName )
        {
            this.roles = roles;
            this.nodes = nodes;
            this.latestReceivedInstanceId = latestReceivedInstanceId;
            this.clusterName = clusterName;
        }

        public Map<InstanceId, URI> getMembers()
        {
            return nodes;
        }

        public Map<String, InstanceId> getRoles()
        {
            return roles;
        }

        public org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId getLatestReceivedInstanceId()
        {
            return latestReceivedInstanceId;
        }

        public String getClusterName()
        {
            return clusterName;
        }
    }

    public static class ConfigurationChangeState
            implements Serializable
    {
        private InstanceId join;
        private URI joinUri;

        private InstanceId leave;

        private String roleWon;
        private InstanceId winner;

        private String roleLost;
        private InstanceId loser;

        public void join( InstanceId join, URI joinUri )
        {
            this.join = join;
            this.joinUri = joinUri;
        }

        public void leave( InstanceId uri )
        {
            this.leave = uri;
        }

        public void elected( String role, InstanceId winner )
        {
            this.roleWon = role;
            this.winner = winner;
        }

        public void unelected( String role, InstanceId unelected )
        {
            roleLost = role;
            loser = unelected;
        }

        public InstanceId getJoin()
        {
            return join;
        }

        public URI getJoinUri()
        {
            return joinUri;
        }

        public InstanceId getLeave()
        {
            return leave;
        }

        public void apply( ClusterContext context )
        {
            if ( join != null )
            {
                context.joined( join, joinUri );
            }

            if ( leave != null )
            {
                context.left( leave );
            }

            if ( roleWon != null )
            {
                context.elected( roleWon, winner );
            }

            if ( roleLost != null )
            {
                context.unelected( roleLost, loser );
            }
        }

        public boolean isLeaving( InstanceId me )
        {
            return me.equals( leave );
        }

        @Override
        public String toString()
        {
            if ( join != null )
            {
                return "Change cluster config, join:" + join;
            }
            if ( leave != null )
            {
                return "Change cluster config, leave:" + leave;
            }

            return "Change cluster config, elected:" + winner + " as " + roleWon;
        }
    }

    public static class ConfigurationTimeoutState
    {
        private final int remainingPings;

        public ConfigurationTimeoutState( int remainingPings)
        {
            this.remainingPings = remainingPings;
        }

        public int getRemainingPings()
        {
            return remainingPings;
        }
    }

}
