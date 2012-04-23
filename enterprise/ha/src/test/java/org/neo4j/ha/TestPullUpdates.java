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
import static org.junit.Assert.fail;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.com.ComException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestPullUpdates
{
    private LocalhostZooKeeperCluster zoo;
    private final HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[3];
    private final TargetDirectory dir = forTest( getClass() );
    private static final int PULL_INTERVAL = 100;
    
    @Before
    public void doBefore() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        for ( int i = 0; i < dbs.length; i++ )
            dbs[i] = newDb( i );
    }

    private HighlyAvailableGraphDatabase newDb( int i )
    {
        return (HighlyAvailableGraphDatabase) new EnterpriseGraphDatabaseFactory().
            newHighlyAvailableDatabaseBuilder( dir.directory( "" + i, true ).getAbsolutePath() ).
            setConfig( HaSettings.server_id, ""+i ).
            setConfig( HaSettings.server, "localhost:" + (6666+i) ).
            setConfig( HaSettings.coordinators, zoo.getConnectionString() ).
            setConfig( HaSettings.pull_interval, PULL_INTERVAL+"ms" ).
            newGraphDatabase();
    }

    @After
    public void doAfter() throws Exception
    {
        for ( HighlyAvailableGraphDatabase db : dbs )
            if ( db != null )
                db.shutdown();
    }
    
    @Test
    public void makeSureUpdatePullerGetsGoingAfterMasterSwitch() throws Exception
    {
        int master = getCurrentMaster();
        setProperty( master, 1 );
        awaitPropagation( 1 );
        kill( master );
        setProperty( awaitNewMaster( master ), 2 );
        start( master );
        awaitPropagation( 2 );
    }

    private int awaitNewMaster( int master ) throws Exception
    {
        int newMaster = getCurrentMaster();
        while ( (newMaster = getCurrentMaster()) == master ) powerNap();
        return newMaster;
    }

    private void powerNap() throws InterruptedException
    {
        Thread.sleep( 50 );
    }

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
                if ( value == null || ((Integer)value).intValue() != i ) ok = false;
            }
            if ( !ok ) powerNap();
        }
        assertTrue( "Change wasn't propagated by pulling updates", ok );
    }
    
    private void setProperty( int dbId, int i ) throws Exception
    {
        awaitHasMaster( dbId );
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

    private void awaitHasMaster( int dbId ) throws Exception
    {
        HighlyAvailableGraphDatabase db = dbs[dbId];
        long endTime = currentTimeMillis() + 10000;
        while ( currentTimeMillis() < endTime )
        {
            try
            {
                db.pullUpdates();
                return;
            }
            catch ( ComException e )
            {   // OK
                powerNap();
            }
        }
        fail( "Master didn't come up" );
    }

    private int getCurrentMaster() throws Exception
    {
        ZooKeeperClusterClient client = new ZooKeeperClusterClient( zoo.getConnectionString() );
        try
        {
            int dbId = client.getMaster().getMachineId();
            awaitBecomeMaster( dbId );
            return dbId;
        }
        finally
        {
            client.shutdown();
        }
    }

    private void awaitBecomeMaster( int dbId ) throws Exception
    {
        HighlyAvailableGraphDatabase db = dbs[dbId];
        long endTime = currentTimeMillis() + 10000;
        while ( !db.isMaster() && currentTimeMillis() < endTime ) powerNap();
        assertTrue( db.isMaster() );
    }
}
