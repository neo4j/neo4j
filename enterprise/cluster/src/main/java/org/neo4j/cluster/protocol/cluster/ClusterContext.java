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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationResponseState;

/**
 * Represents the context necessary for cluster operations. Includes instance membership calls, election
 * facilities and liveness detection. This is expected to be used from a variety of cluster components
 * as part of their state.
 *
 * @see ClusterState
 */
public interface ClusterContext
    extends LoggingContext, TimeoutsContext, ConfigurationContext
{
    int NO_ELECTOR_VERSION = -1;

    // Cluster API
    void addClusterListener( ClusterListener listener );

    void removeClusterListener( ClusterListener listener );

    // Implementation
    void created( String name );

    void joining( String name, Iterable<URI> instanceList );

    void acquiredConfiguration( Map<InstanceId, URI> memberList, Map<String, InstanceId> roles,
                                Set<InstanceId> failedInstances );

    void joined();

    void left();

    void joined( InstanceId instanceId, URI atURI );

    void left( InstanceId node );

    @Deprecated
    void elected( String roleName, InstanceId instanceId );

    void elected( String roleWon, InstanceId winner, InstanceId elector, long version );

    @Deprecated
    void unelected( String roleName, InstanceId instanceId );

    void unelected( String roleLost, InstanceId loser, InstanceId elector, long version );

    ClusterConfiguration getConfiguration();

    boolean isElectedAs( String roleName );

    boolean isInCluster();

    Iterable<URI> getJoiningInstances();

    ObjectOutputStreamFactory getObjectOutputStreamFactory();

    ObjectInputStreamFactory getObjectInputStreamFactory();

    List<ClusterMessage.ConfigurationRequestState> getDiscoveredInstances();

    boolean haveWeContactedInstance( ClusterMessage.ConfigurationRequestState configurationRequested );

    void addContactingInstance( ClusterMessage.ConfigurationRequestState instance, String discoveryHeader );

    String generateDiscoveryHeader();

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

    org.neo4j.cluster.InstanceId getLastElector();

    void setLastElector( InstanceId lastElector );

    long getLastElectorVersion();

    void setLastElectorVersion( long lastElectorVersion );

    boolean shouldFilterContactingInstances();

    /**
     * @return The set of instances present in the failed set. This is not the same as the instances which are
     * determined to be failed based on suspicions, as failed instance information can also come from the cluster
     * configuration response at join time.
     */
    Set<InstanceId> getFailedInstances();
}
