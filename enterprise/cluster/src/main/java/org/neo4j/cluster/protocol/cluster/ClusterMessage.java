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
package org.neo4j.cluster.protocol.cluster;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
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
        private static final long serialVersionUID = -221752558518247157L;

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
            return "ConfigurationRequestState{" + "joiningId=" + joiningId + ", joiningUri=" + joiningUri + "}";
        }
    }

    public static class ConfigurationResponseState
            implements Serializable
    {
        private final Map<InstanceId, URI> nodes;
        private final org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId latestReceivedInstanceId;
        private final Map<String, InstanceId> roles;
        private final String clusterName;

        public ConfigurationResponseState( Map<String, InstanceId> roles, Map<InstanceId, URI> nodes,
                                           org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId latestReceivedInstanceId,
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

        public ConfigurationResponseState snapshot()
        {
            return new ConfigurationResponseState( new HashMap<>(roles), new HashMap<>(nodes),
                    latestReceivedInstanceId, clusterName );
        }

        @Override
        public String toString()
        {
            return "ConfigurationResponseState{" +
                    "nodes=" + nodes +
                    ", latestReceivedInstanceId=" + latestReceivedInstanceId +
                    ", roles=" + roles +
                    ", clusterName='" + clusterName + '\'' +
                    '}';
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

            ConfigurationResponseState that = (ConfigurationResponseState) o;

            if ( clusterName != null ? !clusterName.equals( that.clusterName ) : that.clusterName != null )
            {
                return false;
            }
            if ( latestReceivedInstanceId != null ? !latestReceivedInstanceId.equals( that.latestReceivedInstanceId )
                    : that.latestReceivedInstanceId != null )
            {
                return false;
            }
            if ( nodes != null ? !nodes.equals( that.nodes ) : that.nodes != null )
            {
                return false;
            }
            if ( roles != null ? !roles.equals( that.roles ) : that.roles != null )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = nodes != null ? nodes.hashCode() : 0;
            result = 31 * result + (latestReceivedInstanceId != null ? latestReceivedInstanceId.hashCode() : 0);
            result = 31 * result + (roles != null ? roles.hashCode() : 0);
            result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
            return result;
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

        protected InstanceId getWinner()
        {
            return winner;
        }

        protected String getRoleWon()
        {
            return roleWon;
        }

        protected InstanceId getLoser()
        {
            return loser;
        }

        protected String getRoleLost()
        {
            return roleLost;
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

            if (roleWon != null)
                return "Change cluster config, elected:" + winner + " as " + roleWon;
            else
                return "Change cluster config, unelected:" + loser + " as " + roleWon;
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

            ConfigurationChangeState that = (ConfigurationChangeState) o;

            if ( join != null ? !join.equals( that.join ) : that.join != null )
            {
                return false;
            }
            if ( joinUri != null ? !joinUri.equals( that.joinUri ) : that.joinUri != null )
            {
                return false;
            }
            if ( leave != null ? !leave.equals( that.leave ) : that.leave != null )
            {
                return false;
            }
            if ( loser != null ? !loser.equals( that.loser ) : that.loser != null )
            {
                return false;
            }
            if ( roleLost != null ? !roleLost.equals( that.roleLost ) : that.roleLost != null )
            {
                return false;
            }
            if ( roleWon != null ? !roleWon.equals( that.roleWon ) : that.roleWon != null )
            {
                return false;
            }
            if ( winner != null ? !winner.equals( that.winner ) : that.winner != null )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = join != null ? join.hashCode() : 0;
            result = 31 * result + (joinUri != null ? joinUri.hashCode() : 0);
            result = 31 * result + (leave != null ? leave.hashCode() : 0);
            result = 31 * result + (roleWon != null ? roleWon.hashCode() : 0);
            result = 31 * result + (winner != null ? winner.hashCode() : 0);
            result = 31 * result + (roleLost != null ? roleLost.hashCode() : 0);
            result = 31 * result + (loser != null ? loser.hashCode() : 0);
            return result;
        }
    }

    public static class VersionedConfigurationStateChange extends ConfigurationChangeState
    {
        private InstanceId elector;
        private long version;

        public InstanceId getElector()
        {
            return elector;
        }

        public void setElector( InstanceId elector )
        {
            this.elector = elector;
        }

        public long getVersion()
        {
            return version;
        }

        public void setVersion( long version )
        {
            this.version = version;
        }

        public void apply( ClusterContext context )
        {
            if ( getJoin() != null )
            {
                context.joined( getJoin(), getJoinUri() );
            }

            if ( getLeave() != null )
            {
                context.left( getLeave() );
            }

            if ( getRoleWon() != null )
            {
                context.elected( getRoleWon(), getWinner(), elector, version );
            }

            if ( getRoleLost() != null )
            {
                context.unelected( getRoleLost(), getLoser(), elector, version );
            }
        }

        @Override
        public String toString()
        {
            return "VersionedConfigurationStateChange" +
                   "{elector=" + elector + ", version=" + version + "} " + super.toString();
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

            ConfigurationTimeoutState that = (ConfigurationTimeoutState) o;

            if ( remainingPings != that.remainingPings )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return remainingPings;
        }

        @Override
        public String toString()
        {
            return "ConfigurationTimeoutState{remainingPings=" + remainingPings + "}";
        }
    }

}
