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
package org.neo4j.cluster.member;

import java.net.URI;

import org.neo4j.cluster.ClusterInstanceId;

/**
 * A HighAvailabilityListener is listening for events from elections and availability state.
 * <p/>
 * These are invoked by translating atomic broadcast messages to methods on this interface.
 */
public interface ClusterMemberListener
{
    /**
     * Called when new coordinator has been elected.
     *
     * @param coordinatorId the Id of the coordinator
     */
    void coordinatorIsElected( ClusterInstanceId coordinatorId );

    /**
     * Called when a member announces that it is available to play a particular role, e.g. master or slave.
     * After this it can be assumed that the member is ready to consume messages related to that role.
     *
     * @param role
     * @param availableId the role connection information for the new role holder
     * @param atUri the URI at which the instance is available at
     */
    void memberIsAvailable( String role, ClusterInstanceId availableId, URI atUri );

    /**
     * Called when a member is no longer available for fulfilling a particular role.
     *
     * @param role The role for which the member is unavailable
     * @param unavailableId The id of the member which became unavailable for that role
     */
    void memberIsUnavailable( String role, ClusterInstanceId unavailableId );

    void memberIsFailed( ClusterInstanceId instanceId );

    void memberIsAlive( ClusterInstanceId instanceId );

    public abstract class Adapter
            implements ClusterMemberListener
    {
        @Override
        public void coordinatorIsElected( ClusterInstanceId coordinatorId )
        {
        }

        @Override
        public void memberIsAvailable( String role, ClusterInstanceId availableId, URI atURI )
        {
        }

        @Override
        public void memberIsUnavailable( String role, ClusterInstanceId unavailableId )
        {
        }

        @Override
        public void memberIsFailed( ClusterInstanceId instanceId )
        {
        }

        @Override
        public void memberIsAlive( ClusterInstanceId instanceId )
        {
        }
    }
}
