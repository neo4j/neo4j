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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Loads and initialises any service implementations of <class>UpstreamDatabaseSelectionStrategy</class>.
 * Exposes configured instances of that interface via an iterator.
 */
public class UpstreamDatabaseStrategiesLoader implements Iterable<UpstreamDatabaseSelectionStrategy>
{
    private final TopologyService topologyService;
    private final Config config;
    private final MemberId myself;
    private final Log log;
    private final LogProvider logProvider;

    UpstreamDatabaseStrategiesLoader( TopologyService topologyService, Config config, MemberId myself, LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.config = config;
        this.myself = myself;
        this.log = logProvider.getLog( this.getClass() );
        this.logProvider = logProvider;
    }

    @Override
    public Iterator<UpstreamDatabaseSelectionStrategy> iterator()
    {
        Iterable<UpstreamDatabaseSelectionStrategy> allImplementationsOnClasspath =
                Service.load( UpstreamDatabaseSelectionStrategy.class );

        LinkedHashSet<UpstreamDatabaseSelectionStrategy> candidates = new LinkedHashSet<>();

        for ( String key : config.get( CausalClusteringSettings.upstream_selection_strategy ) )
        {
            for ( UpstreamDatabaseSelectionStrategy candidate : allImplementationsOnClasspath )
            {
                if ( candidate.getKeys().iterator().next().equals( key ) )
                {
                    candidate.inject( topologyService, config, logProvider, myself );
                    candidates.add( candidate );
                }
            }
        }

        log( candidates );

        return candidates.iterator();
    }

    private void log( LinkedHashSet<UpstreamDatabaseSelectionStrategy> candidates )
    {
        log.debug( "Upstream database strategies loaded in order of precedence: " +
                nicelyCommaSeparatedList( candidates ) );
    }

    private static String nicelyCommaSeparatedList( Collection<UpstreamDatabaseSelectionStrategy> items )
    {
        return items.stream()
                .map( UpstreamDatabaseSelectionStrategy::toString )
                .collect( Collectors.joining( ", " ) );
    }
}
