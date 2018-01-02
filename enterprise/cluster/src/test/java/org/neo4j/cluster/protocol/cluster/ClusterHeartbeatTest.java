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
package org.neo4j.cluster.protocol.cluster;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;

public class ClusterHeartbeatTest
        extends ClusterMockTest
{
    @Test
    public void threeNodesJoinAndNoFailures()
            throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 200 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 100, 3 ).
                verifyConfigurations( "after setup", 3000 ).
                leave( 0, 1 ).
                leave( 200, 2 ).
                leave( 200, 3 ) );
    }

    @Test
    public void threeNodesJoinAndThenSlaveDies()
            throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 1000 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 100, 3 ).
                verifyConfigurations( "after setup", 3000 ).
                message( 100, "*** All nodes up and ok" ).
                down( 100, 3 ).
                message( 1000, "*** Should have seen failure by now" ).
                up( 0, 3 ).
                message( 2000, "*** Should have recovered by now" ).
                verifyConfigurations( "after recovery", 0 ).
                leave( 200, 1 ).
                leave( 200, 2 ).
                leave( 200, 3 ) );
    }

    @Test
    public void threeNodesJoinAndThenCoordinatorDies()
            throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 1000 ).
                join( 100, 1, 1 ).
                join( 100, 2, 1 ).
                join( 100, 3, 1 ).
                message( 3000, "*** All nodes up and ok" ).
                down( 500, 1 ).
                message( 1000, "*** Should have seen failure by now" ).
                up( 0, 1 ).
                message( 2000, "*** Should have recovered by now" ).
                verifyConfigurations( "after recovery", 0 ).
                down( 0, 2 ).
                message( 1400, "*** Should have seen failure by now" ).
                up( 0, 2 ).
                message( 800, "*** All nodes leave" ).
                verifyConfigurations( "before leave", 0 ).
                leave( 0, 1 ).
                leave( 300, 2 ).
                leave( 300, 3 ) );
    }

    @Test
    public void threeNodesJoinAndThenCoordinatorDiesForReal()
            throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
        final Map<String, InstanceId> roles = new HashMap<String, InstanceId>();

        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 1000 ).
                join( 100, 1, 1 ).
                join( 100, 2, 1 ).
                join( 100, 3, 1 ).
                message( 3000, "*** All nodes up and ok" ).
                getRoles( roles ).
                down( 800, 1 ).
                message( 2000, "*** Should have seen failure by now" ).
                verifyCoordinatorRoleSwitched( roles ).
                leave( 0, 1 ).
                leave( 300, 2 ).
                leave( 300, 3 ) );
    }
}
