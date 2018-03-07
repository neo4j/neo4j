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
package org.neo4j.causalclustering.core;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.ConnectToRandomCoreServerStrategy;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseSelectionStrategy;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseStrategiesLoader;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseStrategySelector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public abstract class SharedEnterpriseEditionModule extends EditionModule
{

    protected UpstreamDatabaseStrategySelector getUpstreamDatabaseStrategySelector( Config config, TopologyService topologyService, MemberId myself,
            LogProvider logProvider )
    {
        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, config, logProvider, myself );

        UpstreamDatabaseStrategiesLoader loader;
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            loader = new UpstreamDatabaseStrategiesLoader( topologyService, config, myself, logProvider );
            logProvider.getLog( getClass() ).info( "Multi-Data Center option enabled." );
        }
        else
        {
            loader = new NoOpUpstreamDatabaseStrategiesLoader();
        }

        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                new UpstreamDatabaseStrategySelector( defaultStrategy, loader, myself, logProvider );
        return upstreamDatabaseStrategySelector;
    }

    private class NoOpUpstreamDatabaseStrategiesLoader extends UpstreamDatabaseStrategiesLoader
    {
        NoOpUpstreamDatabaseStrategiesLoader()
        {
            super( null, null, null, NullLogProvider.getInstance() );
        }

        @Override
        public Iterator<UpstreamDatabaseSelectionStrategy> iterator()
        {
            return new Iterator<UpstreamDatabaseSelectionStrategy>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public UpstreamDatabaseSelectionStrategy next()
                {
                    throw new NoSuchElementException();
                }
            };
        }
    }
}
