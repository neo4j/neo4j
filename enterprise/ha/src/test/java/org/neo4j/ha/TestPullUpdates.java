/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.TargetDirectory.forTest;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityEvents;
import org.neo4j.kernel.ha.cluster.HighAvailabilityListener;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TargetDirectory;

public class TestPullUpdates
{
    private final TargetDirectory dir = forTest( getClass() );
    private HighlyAvailableGraphDatabase[] dbs;
    private static final int PULL_INTERVAL = 100;
    private static final int SHELL_PORT = 6370;

    private void startDbs( int size, int pullInterval ) throws Exception
    {
        dbs = new HighlyAvailableGraphDatabase[size];
        for ( int i = 0; i < dbs.length; i++ )
        {
            dbs[i] = newDb( i, pullInterval );
        }
    }

    private HighlyAvailableGraphDatabase newDb( int i, int pullInterval )
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( dir.directory( "" + (i + 1), true ).getAbsolutePath() ).
                setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + i) ).
                setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003" ).
                setConfig( HaSettings.server_id, "" + (i + 1) ).
                setConfig( HaSettings.ha_server, "127.0.0.1:" + (6361 + i) ).
                setConfig( HaSettings.pull_interval, pullInterval + "ms" ).
                setConfig( HaSettings.tx_push_factor, "0" ).
                setConfig( ShellSettings.remote_shell_enabled, "true" ).
                setConfig( ShellSettings.remote_shell_port, "" + (SHELL_PORT + 1) ).
                newGraphDatabase();
        Transaction tx = db.beginTx();
        tx.finish();
        return db;
    }

    @After
    public void doAfter() throws Exception
    {
        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    @Test
    public void makeSureUpdatePullerGetsGoingAfterMasterSwitch() throws Exception
    {
        startDbs( 3, PULL_INTERVAL );
        int master = getCurrentMaster();
        setProperty( master, 1 );
        awaitPropagation( 1 );
        awaitNewMaster( (master + 1) % dbs.length );
        kill( master );
        masterElectedLatch.await();
        setProperty( getCurrentMaster(), 2 );
        awaitNewMaster( (master + 1) % dbs.length );
        start( master, PULL_INTERVAL );
        masterElectedLatch.await();
        awaitPropagation( 2 );
    }

    @Test
    public void pullupdatesShellAppPullsUpdates() throws Exception
    {
        startDbs( 2, 0 );
        int master = getCurrentMaster();
        setProperty( master, 1 );
        callPullUpdatesViaShell( (master + 1) % dbs.length );
        awaitPropagation( 1 );
    }

    private void callPullUpdatesViaShell( int i ) throws ShellException
    {
        HighlyAvailableGraphDatabase db = dbs[i];
        ShellClient client = ShellLobby.newClient( SHELL_PORT + i );
        client.evaluate( "pullupdates" );
    }

    private void awaitNewMaster( int master )
    {
        masterElectedLatch = new CountDownLatch( 1 );
        final HighAvailabilityEvents events = dbs[master].getDependencyResolver().resolveDependency(
                HighAvailabilityEvents.class );
        events.addHighAvailabilityEventListener(
                new HighAvailabilityListener.Adapter()

                {
                    @Override
                    public void memberIsAvailable( String role, URI instanceClusterUri, Iterable<URI> instanceUris )
                    {
                        if ( role.equals( HighAvailabilityEvents.MASTER ) )
                        {
                            masterElectedLatch.countDown();
                            events.removeHighAvailabilityEventListener( this );
                        }
                    }
                } );
    }

    private void powerNap() throws InterruptedException
    {
        Thread.sleep( 50 );
    }

    private CountDownLatch masterElectedLatch;


    private void start( int master, int pullInterval )
    {
        dbs[master] = newDb( master, pullInterval );
    }

    private void kill( int master )
    {
        dbs[master].shutdown();
        dbs[master] = null;
    }

    private void awaitPropagation( int i ) throws Exception
    {
        long endTime = currentTimeMillis() + 10000;
        boolean ok = false;
        while ( !ok && currentTimeMillis() < endTime )
        {
            ok = true;
            for ( HighlyAvailableGraphDatabase db : dbs )
            {
                Object value = db.getReferenceNode().getProperty( "i", null );
                if ( value == null || ((Integer) value).intValue() != i )
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

    private void setProperty( int dbId, int i ) throws Exception
    {
        HighlyAvailableGraphDatabase db = dbs[dbId];
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

    private int getCurrentMaster() throws Exception
    {
        for ( int i = 0; i < dbs.length; i++ )
        {
            HighlyAvailableGraphDatabase db = dbs[i];
            if ( db != null && db.isMaster() )
            {
                return i;
            }
        }
        return -1;
    }
}
