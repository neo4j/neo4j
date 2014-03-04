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
package org.neo4j.cluster.protocol.election;

import java.util.List;
import java.util.Set;

import org.neo4j.cluster.ClusterInstanceId;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;

/**
 * Context used by {@link ElectionState}.
 */
public interface ElectionContext
    extends TimeoutsContext, LoggingContext, ConfigurationContext
{

    void created();

    List<ElectionRole> getPossibleRoles();

    /*
     * Removes all roles from the provided node. This is expected to be the first call when receiving a demote
     * message for a node, since it is the way to ensure that election will happen for each role that node had
     */
    void nodeFailed( ClusterInstanceId node );

    Iterable<String> getRoles( ClusterInstanceId server );

    void unelect( String roleName );

    boolean isElectionProcessInProgress( String role );

    void startDemotionProcess( String role, final ClusterInstanceId demoteNode );

    void startElectionProcess( String role );

    void startPromotionProcess( String role, final ClusterInstanceId promoteNode );

    void voted( String role, ClusterInstanceId suggestedNode, Comparable<Object> suggestionCredentials );

    ClusterInstanceId getElectionWinner( String role );

    Comparable<Object> getCredentialsForRole( String role );

    int getVoteCount( String role );

    int getNeededVoteCount();

    void cancelElection( String role );

    Iterable<String> getRolesRequiringElection();

    boolean electionOk();

    boolean isInCluster();

    Iterable<ClusterInstanceId> getAlive();

    boolean isElector();

    boolean isFailed( ClusterInstanceId key );

    ClusterInstanceId getElected( String roleName );

    boolean hasCurrentlyElectedVoted( String role, ClusterInstanceId currentElected );

    Set<ClusterInstanceId> getFailed();
}
