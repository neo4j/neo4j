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
package org.neo4j.harness;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.internal.EnterpriseInProcessServerBuilder;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;

import static java.util.Collections.synchronizedList;
import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

/**
 * Simple main class for manual testing of the complete causal cluster stack, including server etc.
 */
public class CausalClusterInProcessRunner
{

    public static void main( String[] args )
    {
        try
        {
            Path clusterPath = Files.createTempDirectory( "causal-cluster" );
            System.out.println( "clusterPath = " + clusterPath );

            CausalClusterInProcessBuilder.CausalCluster cluster =
                    CausalClusterInProcessBuilder.init()
                            .withCores( 3 )
                            .withReplicas( 3 )
                            .withLogger( toOutputStream( System.out ) )
                            .atPath( clusterPath )
                            .build();

            System.out.println( "Waiting for cluster to boot up..." );
            cluster.boot();

            System.out.println( "Press ENTER to exit..." );
            //noinspection ResultOfMethodCallIgnored
            System.in.read();

            System.out.println( "Shutting down..." );
            cluster.shutdown();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            System.exit( -1 );
        }
        System.exit( 0 );
    }

}
