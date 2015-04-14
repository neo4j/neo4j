/**
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

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.logging.LogProvider;

/**
 * Context used by {@link ElectionState}.
 */
public interface ElectionContext
    extends TimeoutsContext, LogProvider, ConfigurationContext
{

    void created();

    List<ElectionRole> getPossibleRoles();

    /*
     * Removes all roles from the provided node. This is expected to be the first call when receiving a demote
     * message for a node, since it is the way to ensure that election will happen for each role that node had
     */
    void nodeFailed( InstanceId node );

    Iterable<String> getRoles( InstanceId server );

    void unelect( String roleName );

    boolean isElectionProcessInProgress( String role );

    void startDemotionProcess( String role, final InstanceId demoteNode );

    void startElectionProcess( String role );

    void startPromotionProcess( String role, final InstanceId promoteNode );

    public boolean voted( String role, InstanceId suggestedNode, Comparable<Object> suggestionCredentials,
                       long electionVersion );

    InstanceId getElectionWinner( String role );

    Comparable<Object> getCredentialsForRole( String role );

    int getVoteCount( String role );

    int getNeededVoteCount();

    public void forgetElection( String role );

    Iterable<String> getRolesRequiringElection();

    boolean electionOk();

    boolean isInCluster();

    Iterable<InstanceId> getAlive();

    boolean isElector();

    boolean isFailed( InstanceId key );

    InstanceId getElected( String roleName );

    boolean hasCurrentlyElectedVoted( String role, InstanceId currentElected );

    public Set<InstanceId> getFailed();

    public ClusterMessage.VersionedConfigurationStateChange newConfigurationStateChange();

    public VoteRequest voteRequestForRole( ElectionRole role );

    public class VoteRequest implements Serializable
    {
        private static final long serialVersionUID = -715604979485263049L;

        private String role;
        private long version;

        public VoteRequest( String role, long version )
        {
            this.role = role;
            this.version = version;
        }

        public String getRole()
        {
            return role;
        }

        public long getVersion()
        {
            return version;
        }

        @Override
        public String toString()
        {
            return "VoteRequest{role='" + role + "', version=" + version + "}";
        }
    }
}
