/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.protocol.cluster;

import org.junit.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.InstanceId;

public class ClusterHeartbeatTest
        extends ClusterMockTest
{
    @Test
    public void threeNodesJoinAndNoFailures()
            throws URISyntaxException
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
            throws URISyntaxException
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
            throws URISyntaxException
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
            throws URISyntaxException
    {
        final Map<String, InstanceId> roles = new HashMap<>();

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
