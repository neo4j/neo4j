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
package org.neo4j.kernel.index;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;

public class AutoIndexConfigIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void programmaticConfigShouldSurviveMasterSwitches() throws Throwable
    {
        String propertyToIndex = "programmatic-property";

        // Given
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        AutoIndexer<Node> originalAutoIndex = slave.index().getNodeAutoIndexer();
        originalAutoIndex.setEnabled( true );
        originalAutoIndex.startAutoIndexingProperty( propertyToIndex );

        // When
        cluster.shutdown( cluster.getMaster() );
        cluster.await( masterAvailable() );

        // Then
        AutoIndexer<Node> newAutoIndex = slave.index().getNodeAutoIndexer();

        assertThat( newAutoIndex.isEnabled(), is( true ) );
        assertThat( newAutoIndex.getAutoIndexedProperties(), hasItem( propertyToIndex ) );
    }
}
