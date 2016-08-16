/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.discovery;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.edge.EdgeGraphDatabase;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.logging.Level;

import static java.util.stream.Collectors.joining;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class EdgeClusterMember
{
    private final Map<String, String> config = stringMap();
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final File storeDir;
    private EdgeGraphDatabase database;

    EdgeClusterMember( File parentDir, int memberId, DiscoveryServiceFactory discoveryServiceFactory,
                       List<AdvertisedSocketAddress> coreMemberHazelcastAddresses,
                       Map<String, String> extraParams,
                       Map<String, IntFunction<String>> instanceExtraParams,
                       String recordFormat )
    {
        String initialHosts = coreMemberHazelcastAddresses.stream()
                .map( AdvertisedSocketAddress::toString ).collect( joining( "," ) );

        config.put( "dbms.mode", "EDGE" );
        config.put( CoreEdgeClusterSettings.initial_discovery_members.name(), initialHosts );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String, IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( memberId ) );
        }

        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).address.name(), "0.0.0.0:" + (9000 + memberId) );
        config.put( GraphDatabaseSettings.bolt_advertised_address.name(), "127.0.0.1:" + (9000 + memberId) );

        File neo4jHome = new File( parentDir, "server-edge-" + memberId );
        config.put( GraphDatabaseSettings.logs_directory.name(), new File( neo4jHome, "logs" ).getAbsolutePath() );

        this.discoveryServiceFactory = discoveryServiceFactory;
        storeDir = new File( new File( new File( neo4jHome, "data" ), "databases" ), "graph.db" );
        storeDir.mkdirs();
    }

    public void start()
    {
        database = new EdgeGraphDatabase( storeDir, config,
                GraphDatabaseDependencies.newDependencies(), discoveryServiceFactory );
    }

    public void shutdown()
    {
        if ( database != null )
        {
            database.shutdown();
        }
        database = null;
    }

    public EdgeGraphDatabase database()
    {
        return database;
    }

    public File storeDir()
    {
        return storeDir;
    }

}
