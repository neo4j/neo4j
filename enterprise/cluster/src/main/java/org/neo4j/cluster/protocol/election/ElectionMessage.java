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
package org.neo4j.cluster.protocol.election;

import java.io.Serializable;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Messages used to implement the {@link ElectionState}
 */
public enum ElectionMessage
    implements MessageType
{
    created,join,leave,
    demote, performRoleElections, promote, vote, electionTimeout, voted;

    public static class VotedData
        implements Serializable
    {
        private final String role;
        private final InstanceId instanceId;
        private final Comparable<Object> voteCredentials;

        public VotedData( String role, InstanceId instanceId, Comparable<Object> voteCredentials )
        {
            this.role = role;
            this.instanceId = instanceId;
            this.voteCredentials = voteCredentials;
        }

        public String getRole()
        {
            return role;
        }

        public InstanceId getInstanceId()
        {
            return instanceId;
        }

        public Comparable<Object> getVoteCredentials()
        {
            return voteCredentials;
        }
        
        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[role:" + role + ", instance:" + instanceId + ", credentials:" +
                    voteCredentials + ", " + getImplementationSpecificDetails() + "]";
        }

        protected String getImplementationSpecificDetails()
        {
            return "";
        }
    }

    public static class VersionedVotedData extends VotedData
    {
        private final long version;

        public VersionedVotedData( String role, InstanceId instanceId, Comparable<Object> voteCredentials, long version )
        {
            super( role, instanceId, voteCredentials );
            this.version = version;
        }

        public long getVersion()
        {
            return version;
        }

        @Override
        protected String getImplementationSpecificDetails()
        {
            return "version: " + version;
        }
    }
}
