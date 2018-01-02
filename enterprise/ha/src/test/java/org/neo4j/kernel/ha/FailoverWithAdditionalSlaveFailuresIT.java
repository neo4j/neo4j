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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.ha.TestRunConditions;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assume.assumeTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;

@RunWith( Parameterized.class )
public class FailoverWithAdditionalSlaveFailuresIT
{
    @Rule
    public LoggerRule logger = new LoggerRule();
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    // parameters
    private int clusterSize;
    private int[] slavesToFail;

    @Parameters( name = "{index} clusterSize:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {5, new int[]{1}},
                {5, new int[]{2}},
                {5, new int[]{3}},
                {5, new int[]{4}},

                /*
                 * The following cases for 6 and 7 size clusters are too big to consistently verify behaviour. In many
                 * cases the cluster takes longer to form because of cumulative timeouts since the machines we run
                 * these tests on cannot cope with the number of threads spun up. The basic scenario is sufficiently
                 * tested with the 5-size cluster, but the 6 and 7 size cases are good to keep around for posterity,
                 * since a better, multi machine setup can and should test them. Hence, they are ignored through the
                 * JUnit assumption in the @Before method
                 */
                {6, new int[]{1}},
                {6, new int[]{3}},
                {6, new int[]{5}},

                {7, new int[]{1, 2}},
                {7, new int[]{3, 4}},
                {7, new int[]{5, 6}},
        });
    }

    @Before
    public void shouldRun()
    {
        assumeTrue( TestRunConditions.shouldRunAtClusterSize( clusterSize ) );
    }

    public FailoverWithAdditionalSlaveFailuresIT( int clusterSize, int[] slavesToFail )
    {
        this.clusterSize = clusterSize;
        this.slavesToFail = slavesToFail;
    }

    @Test
    public void testFailoverWithAdditionalSlave() throws Throwable
    {
        testFailoverWithAdditionalSlave( clusterSize, slavesToFail );
    }

    private void testFailoverWithAdditionalSlave( int clusterSize, int[] slaveIndexes ) throws Throwable
    {
        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) ).
                withProvider( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( stringMap(
                        ClusterSettings.heartbeat_interval.name(), "1" ) )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getDefaultCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );

            Collection<HighlyAvailableGraphDatabase> failed = new ArrayList<>();
            Collection<RepairKit> repairKits = new ArrayList<>();

            for ( int slaveIndex : slaveIndexes )
            {
                HighlyAvailableGraphDatabase nthSlave = getNthSlave( cluster, slaveIndex );
                failed.add( nthSlave );
                RepairKit repairKit = cluster.fail( nthSlave );
                repairKits.add( repairKit );
            }

            HighlyAvailableGraphDatabase oldMaster = cluster.getMaster();
            failed.add( oldMaster );
            repairKits.add( cluster.fail( oldMaster ) );

            cluster.await( masterAvailable( toArray( failed ) ) );

            for ( RepairKit repairKit : repairKits )
            {
                repairKit.repair();
            }

            Thread.sleep( 3000 ); // give repaired instances a chance to cleanly rejoin and exit faster
        }
        finally
        {
            manager.safeShutdown();
        }
    }

    private HighlyAvailableGraphDatabase getNthSlave( ClusterManager.ManagedCluster cluster, int slaveOrder )
    {
        assert slaveOrder > 0;
        HighlyAvailableGraphDatabase slave = null;

        List<HighlyAvailableGraphDatabase> excluded = new ArrayList<>();
        while( slaveOrder-->0 )
        {
            slave = cluster.getAnySlave( toArray( excluded ) );
            excluded.add( slave );
        }

        return slave;
    }

    private HighlyAvailableGraphDatabase[] toArray( Collection<HighlyAvailableGraphDatabase> excluded )
    {
        return excluded.toArray( new HighlyAvailableGraphDatabase[excluded.size()] );
    }
}
