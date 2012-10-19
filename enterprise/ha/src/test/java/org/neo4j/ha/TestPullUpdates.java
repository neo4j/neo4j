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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.ClusterEventListener;
import org.neo4j.kernel.ha.cluster.ClusterEvents;
import org.neo4j.test.TargetDirectory;

@Ignore
public class TestPullUpdates
{
    private final HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[3];
    private final TargetDirectory dir = forTest( getClass() );
    private static final int PULL_INTERVAL = 100;

    @Before
    public void doBefore() throws Exception
    {
        for ( int i = 0; i < dbs.length; i++ )
        {
            dbs[i] = newDb( i );
        }
    }

    private HighlyAvailableGraphDatabase newDb( int i )
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( dir.directory( "" + (i + 1), true ).getAbsolutePath() ).
                setConfig( HaSettings.server_id, "" + (i + 1) ).
                setConfig( HaSettings.ha_server, "127.0.0.1:" + (6361 + i) ).
                setConfig( HaSettings.cluster_server, "127.0.0.1:" + (5001 + i) ).
                setConfig( HaSettings.initial_hosts, "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003" ).
                setConfig( HaSettings.cluster_discovery_enabled, "false" ).
                setConfig( HaSettings.pull_interval, PULL_INTERVAL + "ms" ).
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
        int master = getCurrentMaster();
        setProperty( master, 1 );
        awaitPropagation( 1 );
        awaitNewMaster( (master + 1) % dbs.length );
        kill( master );
        masterElectedLatch.await();
        setProperty( getCurrentMaster(), 2 );
        awaitNewMaster( (master + 1) % dbs.length );
        start( master );
        masterElectedLatch.await();
        awaitPropagation( 2 );
    }

    private void awaitNewMaster( int master )
    {
        masterElectedLatch = new CountDownLatch( 1 );
        final ClusterEvents events = dbs[master].getDependencyResolver().resolveDependency( ClusterEvents.class );
        events.addClusterEventListener(
                new ClusterEventListener.Adapter()

                {
                    @Override
                    public void memberIsAvailable( String role, URI instanceClusterUri, Iterable<URI> instanceUris )
                    {
                        if ( role.equals( ClusterConfiguration.COORDINATOR ) )
                        {
                            masterElectedLatch.countDown();
                            events.removeClusterEventListener( this );
                        }
                    }
                } );
    }

    private void powerNap() throws InterruptedException
    {
        Thread.sleep( 50 );
    }

    private CountDownLatch masterElectedLatch;


    private void start( int master )
    {
        dbs[master] = newDb( master );
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
