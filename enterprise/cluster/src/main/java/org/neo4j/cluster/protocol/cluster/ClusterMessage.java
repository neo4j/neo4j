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
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId;

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
    configurationRequest, configurationResponse, configurationTimeout, configurationChanged, joinFailure, leaveTimedout;

    public static class ConfigurationResponseState
            implements Serializable
    {
        private List<URI> nodes;
        private InstanceId latestReceivedInstanceId;
        private Map<String, URI> roles;
        private String clusterName;

        public ConfigurationResponseState( Map<String, URI> roles, List<URI> nodes, InstanceId latestReceivedInstanceId,
                                          String clusterName )
        {
            this.roles = roles;
            this.nodes = nodes;
            this.latestReceivedInstanceId = latestReceivedInstanceId;
            this.clusterName = clusterName;
        }

        public List<URI> getMembers()
        {
            return nodes;
        }

        public Map<String, URI> getRoles()
        {
            return roles;
        }

        public InstanceId getLatestReceivedInstanceId()
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
        private URI join;
        private URI leave;

        private String role;
        private URI winner;

        public void join( URI uri )
        {
            this.join = uri;
        }

        public void leave( URI uri )
        {
            this.leave = uri;
        }

        public void elected( String role, URI winner )
        {
            this.role = role;
            this.winner = winner;
        }

        public URI getJoin()
        {
            return join;
        }

        public URI getLeave()
        {
            return leave;
        }

        public void apply( ClusterContext context )
        {
            if ( join != null )
            {
                context.joined( join );
            }

            if ( leave != null )
            {
                context.left( leave );
            }

            if ( role != null )
            {
                context.elected( role, winner );
            }
        }

        public boolean isLeaving( URI me )
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

            return "Change cluster config, elected:" + winner + " as " + role;
        }
    }

}
