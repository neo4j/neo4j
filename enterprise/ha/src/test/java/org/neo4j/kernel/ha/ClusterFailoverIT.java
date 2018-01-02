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
package org.neo4j.kernel.ha;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.ha.TestRunConditions;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assume.assumeTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@RunWith( Parameterized.class )
public class ClusterFailoverIT
{
    @Rule
    public LoggerRule logger = new LoggerRule();
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    // parameters
    private int clusterSize;

    @Parameters( name = "clusterSize:{0}")
    public static Collection<Object[]> data() {
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
        withProvider( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( stringMap(
                    ClusterSettings.heartbeat_interval.name(), "1" ) )
                .build();

        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

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
