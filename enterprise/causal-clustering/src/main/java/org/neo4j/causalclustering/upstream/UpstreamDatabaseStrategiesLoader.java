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

    public UpstreamDatabaseStrategiesLoader( TopologyService topologyService, Config config, MemberId myself, LogProvider logProvider )
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
        Iterable<UpstreamDatabaseSelectionStrategy> allImplementationsOnClasspath = Service.load( UpstreamDatabaseSelectionStrategy.class );

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
        log.debug( "Upstream database strategies loaded in order of precedence: " + nicelyCommaSeparatedList( candidates ) );
    }

    private static String nicelyCommaSeparatedList( Collection<UpstreamDatabaseSelectionStrategy> items )
    {
        return items.stream().map( UpstreamDatabaseSelectionStrategy::toString ).collect( Collectors.joining( ", " ) );
    }
}
