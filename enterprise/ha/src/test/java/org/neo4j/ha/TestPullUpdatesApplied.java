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

import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.test.StreamConsumer;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

/**
 * This test case ensures that updates in HA are first written out to the log
 * and then applied to the store. The problem appears after recovering an
 * unclean shutdown of a slave where no transactions happened (hence the log
 * buffer was not forced). Then it will try to retrieve the latest tx (as
 * written in neostore) from its logs but it will not be present there. This
 * will throw the exception of being unable to find the commit entry for that
 * txid and that will lead to branching. The exception is thrown during startup,
 * before the constructor returns, so we cannot test from userland. Instead we
 * check for the symptom, which is the branched store. This is not nice, just a
 * bit better than checking messages.log for certain entries. Another, more
 * direct, test is present in community.
 */
public class TestPullUpdatesApplied
{
    private LocalhostZooKeeperCluster zoo;
    private final HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[3];
    private final TargetDirectory dir = forTest( getClass() );

    @Before
    public void doBefore() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        for ( int i = 0; i < dbs.length; i++ )
            dbs[i] = newDb( i );
    }

    private HighlyAvailableGraphDatabase newDb( int i )
    {
        return newDb( i, true );
    }

    private HighlyAvailableGraphDatabase newDb( int i, boolean clear )
    {
        return new HighlyAvailableGraphDatabase( dir.directory( "" + i, clear ).getAbsolutePath(), stringMap(
                HaConfig.CONFIG_KEY_SERVER_ID, "" + i, HaConfig.CONFIG_KEY_SERVER, "localhost:" + ( 6666 + i ),
                HaConfig.CONFIG_KEY_COORDINATORS, zoo.getConnectionString(), HaConfig.CONFIG_KEY_PULL_INTERVAL,
                0 + "ms" ) );
    }

    @After
    public void doAfter() throws Exception
    {
        for ( HighlyAvailableGraphDatabase db : dbs )
            if ( db != null ) db.shutdown();
    }

    @Test
    public void testUpdatesAreWritenToLogBeforeBeingAppliedToStore() throws Exception
    {
        int master = getCurrentMaster();
        addNode( master );
        int toKill = ( master + 1 ) % dbs.length;
        HighlyAvailableGraphDatabase dbToKill = dbs[toKill];
        dbToKill.shutdown();
        addNode( master ); // this will be pulled by tne next start up, applied
                           // but not written to log.
        File targetDirectory = dir.directory( "" + toKill, false );
        runInOtherJvmToGetExitCode( new String[] { targetDirectory.getAbsolutePath(), "" + toKill,
                zoo.getConnectionString() } );
        start( toKill, false ); // recovery and branching.
        boolean hasBranchedData = new File( targetDirectory, "branched" ).listFiles().length > 0;
        assertFalse( hasBranchedData );
    }

    // For executing in a different process than the one running the test case.
    public static void main( String[] args ) throws Exception
    {
        int i = Integer.parseInt( args[1] );
        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( args[0], stringMap(
                HaConfig.CONFIG_KEY_SERVER_ID, "" + i, HaConfig.CONFIG_KEY_SERVER, "localhost:" + ( 6666 + i ),
                HaConfig.CONFIG_KEY_COORDINATORS, args[2], HaConfig.CONFIG_KEY_PULL_INTERVAL, 0 + "ms" ) );
        db.pullUpdates(); // this is the bug trigger
        // no shutdown, emulates a crash.
    }

    public static int runInOtherJvmToGetExitCode( String... args ) throws Exception
    {
        List<String> allArgs = new ArrayList<String>( Arrays.asList( "java", "-cp",
                System.getProperty( "java.class.path" ), TestPullUpdatesApplied.class.getName() ) );
        allArgs.addAll( Arrays.asList( args ) );
        Process p = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ) );
        List<Thread> threads = new LinkedList<Thread>();
        launchStreamConsumers( threads, p );
        /*
         * Yes, timeouts suck but HAGD does not terminate politely, since it still has
         * threads running after main() completes, so we need to kill it. When? 5 seconds
         * is good enough.
         */
        Thread.sleep( 5000 );
        p.destroy();
        for ( Thread t : threads )
            t.join();
        return 0;
    }

    private static void launchStreamConsumers( List<Thread> join, Process p )
    {
        InputStream outStr = p.getInputStream();
        InputStream errStr = p.getErrorStream();
        Thread out = new Thread( new StreamConsumer( outStr, System.out, false ) );
        join.add( out );
        Thread err = new Thread( new StreamConsumer( errStr, System.err, false ) );
        join.add( err );
        out.start();
        err.start();
    }

    private void start( int master, boolean clear )
    {
        dbs[master] = newDb( master, clear );
    }

    private long addNode( int dbId )
    {
        HighlyAvailableGraphDatabase db = dbs[dbId];
        long result = -1;
        Transaction tx = db.beginTx();
        try
        {
            result = db.createNode().getId();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return result;
    }

    private int getCurrentMaster()
    {
        ZooKeeperClusterClient client = new ZooKeeperClusterClient( zoo.getConnectionString() );
        try
        {
            return client.getMaster().getMachineId();
        }
        finally
        {
            client.shutdown();
        }
    }
}
