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
package org.neo4j.cluster.member;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.kernel.impl.store.StoreId;

/**
 * A ClusterMemberListener is listening for events from elections and availability state.
 * <p>
 * These are invoked by translating atomic broadcast messages to methods on this interface.
 */
public interface ClusterMemberListener
{
    /**
     * Called when new coordinator has been elected.
     *
     * @param coordinatorId the Id of the coordinator
     */
    void coordinatorIsElected( InstanceId coordinatorId );

    /**
     * Called when a member announces that it is available to play a particular role, e.g. master or slave.
     * After this it can be assumed that the member is ready to consume messages related to that role.
     *
     * @param role
     * @param availableId the role connection information for the new role holder
     * @param atUri the URI at which the instance is available at
     * @param storeId the identifier of a store that became available
     */
    void memberIsAvailable( String role, InstanceId availableId, URI atUri, StoreId storeId );

    /**
     * Called when a member is no longer available for fulfilling a particular role.
     *
     * @param role The role for which the member is unavailable
     * @param unavailableId The id of the member which became unavailable for that role
     */
    void memberIsUnavailable( String role, InstanceId unavailableId );

    /**
     * Called when a member is considered failed, by quorum.
     *
     * @param instanceId of the failed server
     */
    void memberIsFailed( InstanceId instanceId );

    /**
     * Called when a member is considered alive again, by quorum.
     *
     * @param instanceId of the now alive server
     */
    void memberIsAlive( InstanceId instanceId );

    abstract class Adapter implements ClusterMemberListener
    {
        @Override
        public void coordinatorIsElected( InstanceId coordinatorId )
        {
        }

        @Override
        public void memberIsAvailable( String role, InstanceId availableId, URI atURI, StoreId storeId )
        {
        }

        @Override
        public void memberIsUnavailable( String role, InstanceId unavailableId )
        {
        }

        @Override
        public void memberIsFailed( InstanceId instanceId )
        {
        }

        @Override
        public void memberIsAlive( InstanceId instanceId )
        {
        }
    }
}
