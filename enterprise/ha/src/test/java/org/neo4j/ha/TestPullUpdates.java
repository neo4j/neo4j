/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.ha;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class TestPullUpdates
{
    private ClusterManager.ManagedCluster cluster;
    private static final int PULL_INTERVAL = 100;
    private static final int SHELL_PORT = 6370;

    @After
    public void doAfter() throws Throwable
    {
        if ( cluster != null )
        {
            cluster.stop();
        }
    }

    @Test
    @Ignore("Breaks more often than it passes - must wait for a fix")
    public void makeSureUpdatePullerGetsGoingAfterMasterSwitch() throws Throwable
    {
        File root = TargetDirectory.forTest( getClass() ).directory( "makeSureUpdatePullerGetsGoingAfterMasterSwitch", true );
        ClusterManager clusterManager = new ClusterManager( clusterOfSize( 3 ), root, MapUtil.stringMap(
                HaSettings.pull_interval.name(), PULL_INTERVAL+"ms") );
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        setProperty( master, 1 );
        awaitPropagation( 1, cluster );
        cluster.await( masterSeesSlavesAsAvailable( 2 ) );
        ClusterManager.RepairKit masterShutdownRK = cluster.shutdown( master );
        cluster.await( masterAvailable() );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
        setProperty( cluster.getMaster(), 2 );

        masterShutdownRK.repair();
        cluster.await( masterAvailable() );

        cluster.await( masterSeesSlavesAsAvailable( 2 ) );

        awaitPropagation( 2, cluster );
    }

    @Test
    public void pullUpdatesShellAppPullsUpdates() throws Throwable
    {
        File root = TargetDirectory.forTest( getClass() ).directory( "pullUpdatesShellAppPullsUpdates", true );
        Map<Integer, Map<String, String>> instanceConfig = new HashMap<Integer, Map<String, String>>();
        for (int i = 1; i <= 2; i++)
        {
            Map<String, String> thisInstance =
                    MapUtil.stringMap( ShellSettings.remote_shell_port.name(), "" + (SHELL_PORT + i) );
            instanceConfig.put( i, thisInstance );
        }
        ClusterManager clusterManager = new ClusterManager( clusterOfSize( 2 ), root, MapUtil.stringMap(
                HaSettings.pull_interval.name(), "0",
                HaSettings.tx_push_factor.name(), "0" ,
                ShellSettings.remote_shell_enabled.name(), "true"
                ), instanceConfig );
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();

        setProperty( cluster.getMaster(), 1 );
        callPullUpdatesViaShell( 2 );
        assertEquals( 1, cluster.getAnySlave().getReferenceNode().getProperty( "i" ) );
    }

    private void callPullUpdatesViaShell( int i ) throws ShellException
    {
        ShellClient client = ShellLobby.newClient( SHELL_PORT + i );
        client.evaluate( "pullupdates" );
    }

    private void powerNap() throws InterruptedException
    {
        Thread.sleep( 50 );
    }

    private void awaitPropagation( int i, ClusterManager.ManagedCluster cluster ) throws Exception
    {
        long endTime = currentTimeMillis() + PULL_INTERVAL * 20;
        boolean ok = false;
        while ( !ok && currentTimeMillis() < endTime )
        {
            ok = true;
            for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
            {
                Object value = db.getReferenceNode().getProperty( "i", null );
                if ( value == null || (Integer) value != i )
                {
                    ok = false;
                }
            }
            if ( !ok )
            {
                powerNap();
            }
        }
        assertTrue( "Change wasn't propagated by pulling updates", ok );
    }

    private void setProperty( HighlyAvailableGraphDatabase db, int i ) throws Exception
    {
        Transaction tx = db.beginTx();
        try
        {
            db.getReferenceNode().setProperty( "i", i );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
