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
package org.neo4j.ha.upgrade;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.ha.upgrade.Utils.assembleClassPathFromPackage;
import static org.neo4j.ha.upgrade.Utils.downloadAndUnpack;
import static org.neo4j.ha.upgrade.Utils.execJava;
import static org.neo4j.ha.upgrade.Utils.zkConfig;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.tooling.GlobalGraphOperations.at;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.test.TargetDirectory;

public class RollingUpgradeIT
{
    private TargetDirectory DIR = TargetDirectory.forTest( getClass() );
    private File DBS_DIR = DIR.directory( "dbs", true );
    private Process zoo;
    private LegacyDatabase[] legacyDbs;
    private GraphDatabaseAPI[] newDbs;

    @Test
    public void doRollingUpgradeFromOneEightToOneNineWithMasterLast() throws Throwable
    {
        /* High level scenario:
         * 1   Have a cluster of 3 instances running 1.8
         * 1.1 Download a 1.8 package
         * 1.2 Unpack the 1.8 package
         * 1.3 Start ZK cluster, or just one instance even
         * 1.4 Assembly classpath and start 3 JVMs running 1.8
         * 1.5 Create some data in the cluster
         * 2   Go over each one restarting into 1.9
         * 2.1 Grab a JVM and kill it
         * 2.2 Start that db inside this test JVM, which will run 1.9
         * 2.3 Perform a write transaction to the current master and see that it picks it up
         * 2.4 Perform a write transaction to to this instance and see that master picks it up
         * 3   Make sure the cluster functions after each one has been restarted
         * 3.1 Do basic transactions on master/slaves.
         * 3.2 Do a master switch
         * 3.3 Restart one slave
         * 3.4 Take down the instances and do consistency check */
        
        try
        {
            startOneEightCluster();
            rollOverToOneNine();
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
            System.out.println( string );
        else
            System.out.print( string );
    }
    
    @After
    public void cleanUp() throws Exception
    {
        if ( legacyDbs != null )
            for ( LegacyDatabase db : legacyDbs )
                stop( db );
        if ( newDbs != null )
            for ( GraphDatabaseService db : newDbs )
                db.shutdown();
        if ( zoo != null )
            zoo.destroy();
    }

    private void startOneEightCluster() throws Exception
    {
        debug( "Downloading 1.8 package" );
        File oneEightPackage = downloadAndUnpack(
                "http://download.neo4j.org/artifact?edition=enterprise&version=1.8&distribution=zip",
                DIR.directory( "download" ), "1.8-enterprise" );
        String classpath = assembleClassPathFromPackage( oneEightPackage );
        debug( "Starting zoo" );
        zoo = startZoo( classpath );
        debug( "Starting 1.8 cluster in separate jvms" );
        legacyDbs = new LegacyDatabase[3];
        for ( int i = 0; i < legacyDbs.length; i++ )
        {
            legacyDbs[i] = LegacyDatabaseImpl.start( classpath, new File( DBS_DIR, "" + i ), config( i, true ) );
            debug( "  Started " + i );
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
                db.verifyNodeExists( node, name );
        }
        debug( "1.8 cluster fully operational" );
    }

    private Map<String, String> config( int serverId, boolean forOneEight ) throws UnknownHostException
    {
        String localhost = InetAddress.getLocalHost().getHostAddress();
        Map<String, String> result = MapUtil.stringMap(
                HaSettings.server_id.name(), "" + serverId,
                HaSettings.ha_server.name(), localhost + ":" + ( 6000+serverId ),
                ClusterSettings.cluster_server.name(), localhost+":"+( 5000+serverId ),
                "ha.coordinators", localhost + ":2181",
                ClusterSettings.initial_hosts.name(),
                localhost + ":" + 5000 + "," + localhost + ":" + 5001 + "," + localhost + ":" + 5002);
        if ( !forOneEight && serverId != 0 ) // TODO master election algo favors low serverId, default push factor favors high serverId
            result.put( ClusterSettings.allow_init_cluster.name(), Boolean.FALSE.toString() );
        return result;
    }

    private Process startZoo( String classpath ) throws Exception
    {
        return execJava( classpath, "org.apache.zookeeper.server.quorum.QuorumPeerMain", zkConfig( DIR, 1, 2181 ) );
    }

    private void rollOverToOneNine() throws Exception
    {
        debug( "Starting to roll over to 1.9" );
        newDbs = new GraphDatabaseAPI[legacyDbs.length];
        for ( int i = legacyDbs.length - 1; i >= 0; i-- )
        {
            // shut down db
            LegacyDatabase legacyDb = legacyDbs[i];
            String storeDir = legacyDb.getStoreDir();
            stop( legacyDb );
            Thread.sleep( 5000 );

            debug( "Starting it as 1.9" );
            // start that db up in this JVM
            newDbs[i] = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder( storeDir )
                    .setConfig( config( i, false ) )
                    .newGraphDatabase();
            debug( "Started as 1.9" );

            // issue transaction and see that it propagates
            String name = "upgraded-" + i;
            long node = createNodeWithRetry( newDbs[i], name );
            for ( int j = 0; j < i; j++ )
            {
                legacyDbs[j].verifyNodeExists( node, name );
                debug( "Verified on legacy db " + j );
            }
            for ( int j = i; j < newDbs.length; j++ )
            {
                verifyNodeExists( newDbs[j], node, name );
                debug( "Verified on new db " + j );
            }
        }
    }

    private void stop( LegacyDatabase legacyDb )
    {
        try
        {
            legacyDb.stop();
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
                sleep( 1024 );
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
        db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        for ( Node node : at( db ).getAllNodes() )
            if ( name.equals( node.getProperty( "name", null ) ) )
                return;
        fail( "Node " + id + " with name '" + name + "' not found" );
    }
}
