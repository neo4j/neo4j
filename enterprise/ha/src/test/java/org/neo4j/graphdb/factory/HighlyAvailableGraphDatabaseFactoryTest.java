/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.graphdb.factory;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.ha.HaSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class HighlyAvailableGraphDatabaseFactoryTest
{
    @Test
    public void shouldIncludeCorrectSettingsClasses()
    {
        // When
        GraphDatabaseFactoryState state = new HighlyAvailableGraphDatabaseFactory().getCurrentState();

        // Then
        assertThat( state.databaseDependencies().settingsClasses(),
            containsInAnyOrder( GraphDatabaseSettings.class, HaSettings.class, ClusterSettings.class ) );
    }
}
