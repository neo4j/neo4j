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
package org.neo4j.causalclustering.upstream;

import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.upstream_selection_strategy;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class UpstreamDatabaseStrategiesLoaderTest
{

    private MemberId myself = new MemberId( UUID.randomUUID() );

    @Test
    public void shouldReturnConfiguredClassesOnly()
    {
        // given
        Config config = Config.defaults( upstream_selection_strategy, "dummy" );

        UpstreamDatabaseStrategiesLoader strategies =
                new UpstreamDatabaseStrategiesLoader( mock( TopologyService.class ), config, myself, NullLogProvider.getInstance() );

        // when
        Set<UpstreamDatabaseSelectionStrategy> upstreamDatabaseSelectionStrategies = asSet( strategies.iterator() );

        // then
        assertEquals( 1, upstreamDatabaseSelectionStrategies.size() );
        assertEquals( UpstreamDatabaseStrategySelectorTest.DummyUpstreamDatabaseSelectionStrategy.class,
                upstreamDatabaseSelectionStrategies.stream().map( UpstreamDatabaseSelectionStrategy::getClass ).findFirst().get() );
    }

    @Test
    public void shouldReturnTheFirstStrategyThatWorksFromThoseConfigured()
    {
        // given
        Config config = Config.defaults( upstream_selection_strategy, "yet-another-dummy,dummy,another-dummy" );

        // when
        UpstreamDatabaseStrategiesLoader strategies =
                new UpstreamDatabaseStrategiesLoader( mock( TopologyService.class ), config, myself, NullLogProvider.getInstance() );

        // then
        assertEquals( UpstreamDatabaseStrategySelectorTest.YetAnotherDummyUpstreamDatabaseSelectionStrategy.class, strategies.iterator().next().getClass() );
    }
}
