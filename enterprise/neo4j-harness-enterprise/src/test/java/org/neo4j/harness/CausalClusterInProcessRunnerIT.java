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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

public class CausalClusterInProcessRunnerIT
{
    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldBootAndShutdownCluster() throws Exception
    {
        Path clusterPath = testDirectory.absolutePath().toPath();
        CausalClusterInProcessBuilder.PortPickingStrategy portPickingStrategy = new CausalClusterInProcessBuilder.PortPickingStrategy()
        {
            Map<Integer, Integer> cache = new HashMap<>();

            @Override
            public int port( int offset, int id )
            {
                int key = offset + id;
                if ( ! cache.containsKey( key ) )
                {
                    cache.put( key, PortAuthority.allocatePort() );
                }

                return cache.get( key );
            }
        };

        CausalClusterInProcessBuilder.CausalCluster cluster =
                CausalClusterInProcessBuilder.init()
                        .withCores( 3 )
                        .withReplicas( 3 )
                        .withLogger( NullLogProvider.getInstance() )
                        .atPath( clusterPath )
                        .withOptionalPortsStrategy( portPickingStrategy )
                        .build();

        cluster.boot();
        cluster.shutdown();
    }
}
