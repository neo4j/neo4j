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
    public ClusterRule clusterRule = new ClusterRule();

    @Test
    public void programmaticConfigShouldSurviveMasterSwitches()
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
