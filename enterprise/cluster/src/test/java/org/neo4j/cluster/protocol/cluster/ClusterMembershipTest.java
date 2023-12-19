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
package org.neo4j.cluster.protocol.cluster;

import org.junit.Ignore;
import org.junit.Test;

import java.net.URISyntaxException;

/**
 * TODO
 */
public class ClusterMembershipTest
        extends ClusterMockTest
{
    @Test
    public void threeNodesJoinAndThenLeave()
            throws URISyntaxException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 70 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 100, 3 ).
                message( 100, "*** Cluster formed, now leave" ).
                leave( 0, 3 ).
                leave( 100, 2 ).
                leave( 100, 1 ) );
    }

    @Test
    public void threeNodesJoinAndThenLeaveInOriginalOrder()
            throws URISyntaxException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 100 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 100, 3 ).
                message( 100, "*** Cluster formed, now leave" ).
                verifyConfigurations( "starting leave", 0 ).sleep( 100 ).
                leave( 0, 1 ).
                verifyConfigurations( "after 1 left", 200 ).
                leave( 0, 2 ).
                leave( 200, 3 ) );
    }

    @Test
    public void noobTest()
            throws URISyntaxException
    {
        testCluster( 1, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 3 ).
                sleep( 10 ).
                join( 0, 1 ).
                message( 100, "*** Cluster formed, now leave" ).
                leave( 0, 1 ).verifyConfigurations( "after 1 left", 0 ) );
    }

    @Test
    public void sevenNodesJoinAndThenLeave()
            throws URISyntaxException
    {
        testCluster( 7, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 500 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 100, 3 ).
                join( 100, 4 ).
                join( 100, 5 ).
                join( 100, 6 ).
                join( 100, 7 ).
                leave( 100, 7 ).
                leave( 500, 6 ).
                leave( 500, 5 ).
                leave( 500, 4 ).
                leave( 500, 3 ).
                leave( 500, 2 ).
                leave( 500, 1 )
        );
    }

    @Test
    public void oneNodeJoinThenTwoJoinRoughlyAtSameTime()
            throws URISyntaxException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 500 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 10, 3 ).
                message( 2000, "*** All are in " ).
                leave( 0, 3 )
        );
    }

    @Test
    public void oneNodeJoinThenThreeJoinRoughlyAtSameTime2()
            throws URISyntaxException
    {
        testCluster( 4, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 800 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 10, 3 ).
                join( 10, 4 ).
                message( 2000, "*** All are in " ).
                broadcast( 10, 2, "Hello world" )
        );
    }

    @Test
    public void twoNodesJoinThenOneLeavesAsThirdJoins()
            throws URISyntaxException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 820 ).
                join( 0, 1 ).
                join( 10, 2 ).
                message( 80, "*** 1 and 2 are in cluster" ).
                leave( 10, 2 ).
                join( 20, 3 )
        );
    }

    @Test
    @Ignore( "instance 1 is in start, 2 in discovery. Correct but we don't have a way to verify it yet" )
    public void oneNodeCreatesClusterAndThenAnotherJoinsAsFirstLeaves()
            throws URISyntaxException
    {
        testCluster( 2, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 1000 ).
                join( 0, 1 ).
                join( 10, 2, 1, 2 ).
                leave( 20, 1 )
        );
    }

    @Test
    public void threeNodesJoinAndThenFirstLeavesAsFourthJoins()
            throws URISyntaxException
    {
        testCluster( 4, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 200 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 100, 3 ).
                message( 100, "*** Cluster formed, now leave" ).
                leave( 0, 1 ).
                join( 10, 4 )
        );
    }

    @Test
    public void threeNodesJoinAndThenFirstLeavesAsFourthJoins2()
            throws URISyntaxException
    {
        testCluster( 5, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 200 ).
                join( 100, 1 ).
                join( 100, 2 ).
                join( 100, 3 ).
                join( 100, 4 ).
                message( 100, "*** Cluster formed, now leave" ).
                leave( 0, 1 ).
                join( 30, 5 ).
                leave( 0, 2 )
        );
    }

    @Ignore( "Ignore until fix available" )
    @Test
    public void threeNodesJoinAtSameTime()
            throws URISyntaxException
    {
        testCluster( 3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 400 ).
                join( 0, 1, 1, 2, 3 ).
                join( 0, 2, 1, 2, 3 ).
                join( 0, 3, 1, 2, 3 ).
                message( 390, "*** Cluster formed" ));
    }
}
