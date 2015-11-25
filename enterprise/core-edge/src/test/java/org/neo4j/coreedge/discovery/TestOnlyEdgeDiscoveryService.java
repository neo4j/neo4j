/*
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
package org.neo4j.coreedge.discovery;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class TestOnlyEdgeDiscoveryService extends LifecycleAdapter implements EdgeDiscoveryService
{
    private final InstanceId edgeMe;
    private final TestOnlyDiscoveryServiceFactory cluster;

    public TestOnlyEdgeDiscoveryService( Config config, TestOnlyDiscoveryServiceFactory cluster )
    {
        this.cluster = cluster;
        this.edgeMe = toEdge( config );
        cluster.edgeMembers.add( edgeMe );
    }

    private InstanceId toEdge( Config config )
    {
        return new InstanceId( Integer.valueOf( config.getParams().get( ClusterSettings.server_id.name() ) ) );
    }

    @Override
    public void stop()
    {
        cluster.edgeMembers.remove( edgeMe );
    }

    @Override
    public ClusterTopology currentTopology()
    {
        return new TestOnlyClusterTopology( false, cluster.coreMembers, cluster.edgeMembers );
    }
}
