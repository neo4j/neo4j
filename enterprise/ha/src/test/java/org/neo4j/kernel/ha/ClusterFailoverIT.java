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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.ha.TestRunConditions;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.rule.LoggerRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assume.assumeTrue;

@RunWith( Parameterized.class )
public class ClusterFailoverIT
{
    @Rule
    public LoggerRule logger = new LoggerRule();
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();

    // parameters
    private int clusterSize;

    @Parameters( name = "clusterSize:{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][] {
                { 3 },
                { 4 },
                { 5 },
                { 6 },
                { 7 },
        });
    }

    public ClusterFailoverIT( int clusterSize )
    {
        this.clusterSize = clusterSize;
    }

    private void testFailOver( int clusterSize ) throws Throwable
    {
        // given
        ClusterManager clusterManager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "failover" ) ).
                withCluster( ClusterManager.clusterOfSize( clusterSize ) ).build();

        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getCluster();

        cluster.await( ClusterManager.allSeesAllAsAvailable() );
        HighlyAvailableGraphDatabase oldMaster = cluster.getMaster();

        // When
        long start = System.nanoTime();
        ClusterManager.RepairKit repairKit = cluster.fail( oldMaster );
        logger.getLogger().warning( "Shut down master" );

        // Then
        cluster.await( ClusterManager.masterAvailable( oldMaster ) );
        long end = System.nanoTime();

        logger.getLogger().warning( "Failover took:" + (end - start) / 1000000 + "ms" );

        repairKit.repair();
        Thread.sleep( 3000 ); // give repaired instance chance to cleanly rejoin and exit faster

        clusterManager.safeShutdown();
    }

    @Test
    public void testFailOver() throws Throwable
    {
        assumeTrue( TestRunConditions.shouldRunAtClusterSize( clusterSize ) );
        testFailOver( clusterSize );
    }
}
