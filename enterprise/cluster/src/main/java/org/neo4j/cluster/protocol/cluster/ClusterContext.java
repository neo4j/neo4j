/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationResponseState;

/**
 * Context for cluster API state machine
 *
 * @see ClusterState
 */
public interface ClusterContext
    extends LoggingContext, TimeoutsContext, ConfigurationContext
{

    // Cluster API
    void addClusterListener( ClusterListener listener );

    void removeClusterListener( ClusterListener listener );

    // Implementation
    void created( String name );

    void joining( String name, Iterable<URI> instanceList );

    void acquiredConfiguration( final Map<InstanceId, URI> memberList, final Map<String, InstanceId> roles );

    void joined();

    void left();

    void joined( final InstanceId instanceId, final URI atURI );

    void left( final InstanceId node );

    void elected( final String roleName, final InstanceId instanceId );

    void unelected( final String roleName, final InstanceId instanceId );

    ClusterConfiguration getConfiguration();

    boolean isElectedAs( String roleName );

    boolean isInCluster();

    Iterable<URI> getJoiningInstances();

    ObjectOutputStreamFactory getObjectOutputStreamFactory();

    ObjectInputStreamFactory getObjectInputStreamFactory();

    List<ClusterMessage.ConfigurationRequestState> getDiscoveredInstances();

    void setBoundAt( URI boundAt );

    void joinDenied( ConfigurationResponseState configurationResponseState );

    boolean hasJoinBeenDenied();
    
    ConfigurationResponseState getJoinDeniedConfigurationResponseState();

    Iterable<InstanceId> getOtherInstances();

    boolean isInstanceJoiningFromDifferentUri( InstanceId joiningId, URI joiningUri );

    void instanceIsJoining( InstanceId joiningId, URI uri );

    String myName();

    void discoveredLastReceivedInstanceId( long id );

    boolean isCurrentlyAlive( InstanceId joiningId );

    long getLastDeliveredInstanceId();
}
