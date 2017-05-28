/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.identity.ClusterId;

/**
 * Extends upon the topology service with a few extra services, connected to
 * the underlying discovery service.
 */
public interface CoreTopologyService extends TopologyService
{
    void addCoreTopologyListener( Listener listener );

    /**
     * Publishes the cluster ID so that other members might discover it.
     * Should only succeed to publish if one missing or already the same (CAS logic).
     *
     * @param clusterId The cluster ID to publish.
     *
     * @return True if the cluster ID was successfully CAS:ed, otherwise false.
     */
    boolean setClusterId( ClusterId clusterId ) throws InterruptedException;

    interface Listener
    {
        void onCoreTopologyChange( CoreTopology coreTopology );
    }
}
