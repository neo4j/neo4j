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
package org.neo4j.causalclustering.readreplica;

import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class UpstreamDatabaseStrategiesLoaderTest
{

    private MemberId myself = new MemberId( UUID.randomUUID() );

    @Test
    public void shouldReturnConfiguredClassesOnly() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment( stringMap( "causal_clustering.upstream_selection_strategy", "dummy" ) );

        UpstreamDatabaseStrategiesLoader strategies =
                new UpstreamDatabaseStrategiesLoader( mock( TopologyService.class ), config,
                        myself, NullLogProvider.getInstance() );

        // when
        Set<UpstreamDatabaseSelectionStrategy> upstreamDatabaseSelectionStrategies = asSet( strategies.iterator() );

        // then
        assertEquals( 1, upstreamDatabaseSelectionStrategies.size() );
        assertEquals( UpstreamDatabaseStrategySelectorTest.DummyUpstreamDatabaseSelectionStrategy.class,
                upstreamDatabaseSelectionStrategies.stream().map( UpstreamDatabaseSelectionStrategy::getClass ).findFirst().get() );
    }

    @Test
    public void shouldReturnTheFirstStrategyThatWorksFromThoseConfigured() throws Exception
    {
        // given
        Config config = Config.defaults();
        config.augment(
                stringMap( "causal_clustering.upstream_selection_strategy", "yet-another-dummy,dummy,another-dummy" ) );

        // when
        UpstreamDatabaseStrategiesLoader strategies =
                new UpstreamDatabaseStrategiesLoader( mock( TopologyService.class ), config,
                        myself, NullLogProvider.getInstance() );

        // then
        assertEquals( UpstreamDatabaseStrategySelectorTest.YetAnotherDummyUpstreamDatabaseSelectionStrategy.class,
                strategies.iterator().next().getClass() );
    }
}
