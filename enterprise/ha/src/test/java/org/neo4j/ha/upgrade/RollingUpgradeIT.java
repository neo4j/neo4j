/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ha.upgrade;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.test.TargetDirectory;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.fail;

import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.ha.upgrade.Utils.assembleClassPathFromPackage;
import static org.neo4j.ha.upgrade.Utils.downloadAndUnpack;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.kernel.ha.HaSettings.ha_server;
import static org.neo4j.tooling.GlobalGraphOperations.at;

@Ignore( "Keep this test around as it's a very simple and 'close' test to quickly verify rolling upgrades" )
public class RollingUpgradeIT
{
    private static final String OLD_VERSION = "2.0.0";
    
    private final TargetDirectory DIR = TargetDirectory.forTest( getClass() );
    private final File DBS_DIR = DIR.cleanDirectory( "dbs" );
    private LegacyDatabase[] legacyDbs;
    private GraphDatabaseAPI[] newDbs;

    @Test
    public void doRollingUpgradeFromPreviousVersionWithMasterLast() throws Throwable
    {
        /* High level scenario:
         * 1   Have a cluster of 3 instances running <old version>
         * 1.1 Download a <old version> package
         * 1.2 Unpack the <old version> package
         * 1.4 Assembly classpath and start 3 JVMs running <old version>
         * 1.5 Create some data in the cluster
         * 2   Go over each one restarting into <this version>
         * 2.1 Grab a JVM and kill it
         * 2.2 Start that db inside this test JVM, which will run <this version>
         * 2.3 Perform a write transaction to the current master and see that it picks it up
         * 2.4 Perform a write transaction to to this instance and see that master picks it up
         * 3   Make sure the cluster functions after each one has been restarted
         * 3.1 Do basic transactions on master/slaves.
         * 3.2 Do a master switch
         * 3.3 Restart one slave
         * 3.4 Take down the instances and do consistency check */
        
        try
        {
            startOldVersionCluster();
            rollOverToNewVersion();
            verify();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    private void debug( String message )
    {
        debug( message, true );
    }
    
    private void debug( String message, boolean enter )
    {
        String string = "RUT " + message;
        if ( enter )
        {
            System.out.println( string );
        }
        else
        {
            System.out.print( string );
        }
    }
    
    @After
    public void cleanUp() throws Exception
    {
        if ( legacyDbs != null )
        {
            for ( int i = 0; i < legacyDbs.length; i++ )
            {
                stop( i );
            }
        }
        if ( newDbs != null )
        {
            for ( GraphDatabaseService db : newDbs )
            {
                db.shutdown();
            }
        }
    }

    private void startOldVersionCluster() throws Exception
    {
        debug( "Downloading " + OLD_VERSION + " package" );
        File oldVersionPackage = downloadAndUnpack(
                "http://download.neo4j.org/artifact?edition=enterprise&version=" + OLD_VERSION + "&distribution=zip",
                DIR.cacheDirectory( "download" ), OLD_VERSION + "-enterprise" );
        String classpath = assembleClassPathFromPackage( oldVersionPackage );
        debug( "Starting " + OLD_VERSION + " cluster in separate jvms" );
        @SuppressWarnings( "rawtypes" )
        Future[] legacyDbFutures = new Future[3];
        for ( int i = 0; i < legacyDbFutures.length; i++ )
        {
            legacyDbFutures[i] = LegacyDatabaseImpl.start( classpath, new File( DBS_DIR, "" + i ), config( i ) );
            debug( "  Started " + i );
        }
        legacyDbs = new LegacyDatabase[legacyDbFutures.length];
        for ( int i = 0; i < legacyDbFutures.length; i++ )
        {
            legacyDbs[i] = (LegacyDatabase) legacyDbFutures[i].get();
        }
        
        for ( LegacyDatabase db : legacyDbs )
        {
            debug( "  Awaiting " + db.getStoreDir() + " to start" );
            db.awaitStarted( 10, TimeUnit.SECONDS );
            debug( "  " + db.getStoreDir() + " fully started" );
        }
        for ( int i = 0; i < legacyDbs.length; i++ )
        {
            String name = "initial-" + i;
            long node = legacyDbs[i].createNode( name );
            for ( LegacyDatabase db : legacyDbs )
            {
                db.verifyNodeExists( node, name );
            }
        }
        debug( OLD_VERSION + " cluster fully operational" );
    }

    private Map<String, String> config( int serverId ) throws UnknownHostException
    {
        String localhost = InetAddress.getLocalHost().getHostAddress();
        Map<String, String> result = MapUtil.stringMap(
                server_id.name(), "" + serverId,
                cluster_server.name(), localhost + ":" + ( 5000+serverId ),
                ha_server.name(), localhost + ":" + ( 6000+serverId ),
                initial_hosts.name(), localhost + ":" + 5000 + "," + localhost + ":" + 5001 + "," + localhost + ":" + 5002 );
        return result;
    }

    private void rollOverToNewVersion() throws Exception
    {
        debug( "Starting to roll over to current version" );
        Pair<LegacyDatabase, Integer> master = findOutWhoIsMaster();
        newDbs = new GraphDatabaseAPI[legacyDbs.length];
        for ( int i = 0; i < legacyDbs.length; i++ )
        {
            LegacyDatabase legacyDb = legacyDbs[i];
            if ( legacyDb == master.first() )
            {   // Roll over the master last
                continue;
            }
            
            rollOver( legacyDb, i );
        }
        rollOver( master.first(), master.other() );
    }

    private void rollOver( LegacyDatabase legacyDb, int i ) throws Exception
    {
        String storeDir = legacyDb.getStoreDir();
        stop( i );
        Thread.sleep( 30000 );

        debug( "Starting " + i + " as current version" );
        // start that db up in this JVM
        newDbs[i] = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( storeDir )
                .setConfig( config( i ) )
                .newGraphDatabase();
        debug( "Started " + i + " as current version" );
        legacyDbs[i] = null;

        // issue transaction and see that it propagates
        String name = "upgraded-" + i;
        long node = createNodeWithRetry( newDbs[i], name );
        debug( "Node created on " + i );
        for ( int j = 0; j < i; j++ )
        {
            if ( legacyDbs[i] != null )
            {
                legacyDbs[j].verifyNodeExists( node, name );
                debug( "Verified on legacy db " + j );
            }
        }
        for ( int j = 0; j < newDbs.length; j++ )
        {
            if ( newDbs[j] != null )
            {
                verifyNodeExists( newDbs[j], node, name );
                debug( "Verified on new db " + j );
            }
        }
    }

    private Pair<LegacyDatabase,Integer> findOutWhoIsMaster()
    {
        try
        {
            for ( int i = 0; i < legacyDbs.length; i++ )
            {
                LegacyDatabase db = legacyDbs[i];
                if ( db.isMaster() )
                {
                    return Pair.of( db, i );
                }
            }
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
        throw new IllegalStateException( "No master" );
    }

    private void stop( int i )
    {
        try
        {
            LegacyDatabase legacyDb = legacyDbs[i];
            if ( legacyDb != null )
            {
                legacyDb.stop();
                legacyDbs[i] = null;
            }
        }
        catch ( RemoteException e )
        {
            // OK
        }
    }

    private void verify( )
    {
        // TODO Auto-generated method stub
        
    }

    private long createNodeWithRetry( GraphDatabaseService db, String name ) throws InterruptedException
    {
        long end = currentTimeMillis() + SECONDS.toMillis( 60*2 );
        Exception exception = null;
        while ( currentTimeMillis() < end )
        {
            try
            {
                return createNode( db, name );
            }
            catch ( Exception e )
            {
                // OK
                exception = e;
                // Jiffy is a less well known SI unit for time equal to 1024 millis, aka binary second
                debug( "Master not switched yet, retrying in a jiffy (" + e + ")" );
                sleep( 1024 ); // 1024, because why the hell not
            }
        }
        throw launderedException( exception );
    }

    private long createNode( GraphDatabaseService db, String name )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.setProperty( "name", name );
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }

    private void verifyNodeExists( GraphDatabaseAPI db, long id, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            for ( Node node : at( db ).getAllNodes() )
            {
                if ( name.equals( node.getProperty( "name", null ) ) )
                {
                    return;
                }
            }
            tx.success();
        }
        fail( "Node " + id + " with name '" + name + "' not found" );
    }
}
