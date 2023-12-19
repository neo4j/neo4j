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
package org.neo4j.harness;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

public class ClusterOfClustersInProcessRunner
{

    public static void main( String[] args )
    {
        try
        {
            Path clusterPath = Files.createTempDirectory( "causal-cluster" );
            System.out.println( "clusterPath = " + clusterPath );

            CausalClusterInProcessBuilder.CausalCluster cluster =
                    CausalClusterInProcessBuilder.init()
                            .withCores( 6 )
                            .withReplicas( 4 )
                            .withLogger( toOutputStream( System.out ) )
                            .atPath( clusterPath )
                            .withOptionalDatabases( Arrays.asList("foo", "bar") )
                            .build();

            System.out.println( "Waiting for cluster to boot up..." );
            cluster.boot();

            System.out.println( "Press ENTER to exit ..." );
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
