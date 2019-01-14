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
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.causalclustering.identity.ClusterId;

/**
 * Extends upon the topology service with a few extra services, connected to
 * the underlying discovery service.
 */
public interface CoreTopologyService extends TopologyService
{
    void addLocalCoreTopologyListener( Listener listener );

    void removeLocalCoreTopologyListener( Listener listener );

    /**
     * Publishes the cluster ID so that other members might discover it.
     * Should only succeed to publish if one missing or already the same (CAS logic).
     *
     * @param clusterId The cluster ID to publish.
     *
     * @return True if the cluster ID was successfully CAS:ed, otherwise false.
     */
    boolean setClusterId( ClusterId clusterId, String dbName ) throws InterruptedException;

    /**
     * Sets or updates the leader memberId for the given database (i.e. Raft consensus group).
     * This is intended for informational purposes **only**, e.g. in {@link ClusterOverviewProcedure}.
     * The leadership information should otherwise be communicated via raft as before.
     * @param leaderInfo Information about the new leader
     * @param dbName The database name for which memberId is the new leader
     */
    void setLeader( LeaderInfo leaderInfo, String dbName );

    /**
     * Set the leader memberId to null for a given database (i.e. Raft consensus group).
     * This is intended to trigger state cleanup for informational procedures like {@link ClusterOverviewProcedure}
     *
     * @param term The term for which this topology member should handle a stepdown
     * @param dbName The database for which this topology member should handle a stepdown
     */
    void handleStepDown( long term, String dbName );

    interface Listener
    {
        void onCoreTopologyChange( CoreTopology coreTopology );
        String dbName();
    }
}
