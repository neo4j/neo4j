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
package org.neo4j.graphdb.factory;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.ha.HaSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class HighlyAvailableGraphDatabaseFactoryTest
{
    @Test
    public void shouldIncludeCorrectSettingsClasses() throws Throwable
    {
        // When
        GraphDatabaseFactoryState state = new HighlyAvailableGraphDatabaseFactory().getCurrentState();

        // Then
        assertThat( state.databaseDependencies().settingsClasses(),
            containsInAnyOrder( GraphDatabaseSettings.class, HaSettings.class, ClusterSettings.class ) );
    }
}