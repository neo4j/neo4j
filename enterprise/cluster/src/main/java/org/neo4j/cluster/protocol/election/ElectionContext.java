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
package org.neo4j.cluster.protocol.election;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;

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
    void nodeFailed( InstanceId node );

    Iterable<String> getRoles( InstanceId server );

    boolean isElectionProcessInProgress( String role );

    void startElectionProcess( String role );

    boolean voted( String role, InstanceId suggestedNode, ElectionCredentials suggestionCredentials,
                       long electionVersion );

    InstanceId getElectionWinner( String role );

    ElectionCredentials getCredentialsForRole( String role );

    int getVoteCount( String role );

    int getNeededVoteCount();

    void forgetElection( String role );

    Iterable<String> getRolesRequiringElection();

    boolean electionOk();

    boolean isInCluster();

    Iterable<InstanceId> getAlive();

    boolean isElector();

    boolean isFailed( InstanceId key );

    InstanceId getElected( String roleName );

    boolean hasCurrentlyElectedVoted( String role, InstanceId currentElected );

    Set<InstanceId> getFailed();

    ClusterMessage.VersionedConfigurationStateChange newConfigurationStateChange();

    VoteRequest voteRequestForRole( ElectionRole role );

    class VoteRequest implements Serializable
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
