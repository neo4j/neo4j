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

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public abstract class UpstreamDatabaseSelectionStrategy extends Service
{
    protected TopologyService topologyService;
    protected Config config;
    protected Log log;
    protected MemberId myself;
    protected String readableName;
    protected String dbName;

    public UpstreamDatabaseSelectionStrategy( String key, String... altKeys )
    {
        super( key, altKeys );
    }

    // Service loader can't inject via the constructor
    public void inject( TopologyService topologyService, Config config, LogProvider logProvider, MemberId myself )
    {
        this.topologyService = topologyService;
        this.config = config;
        this.log = logProvider.getLog( this.getClass() );
        this.myself = myself;
        this.dbName = config.get( CausalClusteringSettings.database );

        readableName = StreamSupport.stream( getKeys().spliterator(), false ).collect( Collectors.joining( ", " ) );
        log.info( "Using upstream selection strategy " + readableName );
        init();
    }

    public void init()
    {
    }

    public abstract Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException;

    @Override
    public String toString()
    {
        return nicelyCommaSeparatedList( getKeys() );
    }

    private static String nicelyCommaSeparatedList( Iterable<String> keys )
    {
        return StreamSupport.stream( keys.spliterator(), false ).collect( Collectors.joining( ", " ) );
    }
}
