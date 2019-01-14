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
package org.neo4j.ha.upgrade;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.ha.upgrade.Utils.assembleClassPathFromPackage;
import static org.neo4j.ha.upgrade.Utils.downloadAndUnpack;
import static org.neo4j.kernel.ha.HaSettings.ha_server;

@Ignore( "Keep this test around as it's a very simple and 'close' test to quickly verify rolling upgrades" )
@RunWith( Parameterized.class )
public class RollingUpgradeIT
{
    private static final int CLUSTER_SIZE = 3;

    public static final RelationshipType type1 = RelationshipType.withName( "type1" );
    public static final RelationshipType type2 = RelationshipType.withName( "type2" );

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private LegacyDatabase[] legacyDbs;
    private GraphDatabaseAPI[] newDbs;
    private long centralNode;

    private final String oldVersion;

    public RollingUpgradeIT( String oldVersion )
    {
        this.oldVersion = oldVersion;
    }

    @Parameters
    public static Iterable<Object[]> oldVersions()
    {
        return Arrays.asList( new Object[][]{
                {"2.1.2"}
        } );
    }

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
            shutdownAndDoConsistencyChecks();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw e;
        }
    }

    private void shutdownAndDoConsistencyChecks()
    {
/*
        Collection<String> storeDirs = new ArrayList<>( newDbs.length );
        for ( GraphDatabaseAPI item : newDbs )
        {
            if ( item != null )
            {
                storeDirs.add( item.getStoreDir() );
                item.shutdown();
            }
        }

        ConsistencyCheckService service = new ConsistencyCheckService();
        for ( String storeDir : storeDirs )
        {
            service.runFullConsistencyCheck( storeDir, Config.defaults(),
                    ProgressMonitorFactory.textual(System.out), StringLogger.SYSTEM );
        }
*/
    }

    private void debug( String message )
    {
        debug( message, true );
    }

    private void debug( String message, boolean enter )
    {
        // TODO come on, tests should not output to the screen.
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
    public void cleanUp()
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
                if ( db != null )
                {
                    db.shutdown();
                }
            }
        }
    }

    private void startOldVersionCluster() throws Exception
    {
        debug( "Downloading " + oldVersion + " package" );
        File oldVersionPackage = downloadAndUnpack(
                "http://neo4j.com/customer/download/neo4j-enterprise-" + oldVersion + "-windows.zip",
                testDirectory.directory( "download" ), oldVersion + "-enterprise" );
        String classpath = assembleClassPathFromPackage( oldVersionPackage );
        debug( "Starting " + oldVersion + " cluster in separate jvms" );
        List<Future<LegacyDatabase>> legacyDbFutures = new ArrayList<>( CLUSTER_SIZE );
        for ( int i = 0; i < CLUSTER_SIZE; i++ )
        {
            Future<LegacyDatabase> dbStart = LegacyDatabaseImpl.start( classpath, storeDir( i ), config( i ) );
            legacyDbFutures.add( dbStart );
            debug( "  Started " + i );
        }
        legacyDbs = new LegacyDatabase[CLUSTER_SIZE];
        for ( int i = 0; i < CLUSTER_SIZE; i++ )
        {
            legacyDbs[i] = legacyDbFutures.get( i ).get();
        }

        for ( LegacyDatabase db : legacyDbs )
        {
            debug( "  Awaiting " + db.getStoreDir() + " to start" );
            db.awaitStarted( 10, TimeUnit.SECONDS );
            debug( "  " + db.getStoreDir() + " fully started" );
        }
        for ( LegacyDatabase legacyDb : legacyDbs )
        {
            long node = legacyDb.createNode();
            for ( LegacyDatabase db : legacyDbs )
            {
                db.verifyNodeExists( node );
            }
        }
        debug( oldVersion + " cluster fully operational" );

        centralNode = legacyDbs[0].initialize();
    }

    private File storeDir( int serverId )
    {
        return new File( testDirectory.directory( "dbs" ), "" + serverId );
    }

    private Map<String, String> config( int serverId ) throws UnknownHostException
    {
        String localhost = localhost();
        Map<String, String> result = MapUtil.stringMap(
                server_id.name(), "" + serverId,
                cluster_server.name(), localhost + ":" + (5000 + serverId),
                ha_server.name(), localhost + ":" + (6000 + serverId),
                GraphDatabaseSettings.allow_upgrade.name(), "true",
                GraphDatabaseSettings.pagecache_memory.name(), "8m",
                OnlineBackupSettings.online_backup_server.name(), localhost + ":" + backupPort( serverId ),
                initial_hosts.name(), localhost + ":" + 5000 + "," + localhost + ":" + 5001 + "," + localhost + ":" + 5002 );
        return result;
    }

    private String localhost() throws UnknownHostException
    {
        return InetAddress.getLocalHost().getHostAddress();
    }

    private int backupPort( int serverId )
    {
        return 6362 + serverId;
    }

    private void rollOverToNewVersion() throws Exception
    {
        debug( "Starting to roll over to current version" );
        Pair<LegacyDatabase, Integer> master = findOutWhoIsMaster();
        newDbs = new GraphDatabaseAPI[legacyDbs.length];
        int authorativeSlaveId = -1;
        for ( int i = 0; i < legacyDbs.length; i++ )
        {
            LegacyDatabase legacyDb = legacyDbs[i];
            if ( legacyDb == master.first() )
            {   // Roll over the master last
                debug( "master is " + master.first().getStoreDir() );
                continue;
            }

            rollOver( legacyDb, i, master.other(), authorativeSlaveId );
            if ( authorativeSlaveId == -1 )
            {
                authorativeSlaveId = i;
            }
        }
        rollOver( master.first(), master.other(), master.other(), -2 );
    }

    private void rollOver( LegacyDatabase legacyDb, int i, int masterServerId, int authorativeSlaveId )
            throws Exception
    {
        String storeDir = legacyDb.getStoreDir();
        if ( i == 0 )
        {
            storeDir += "new";
        }
        stop( i );

        File storeDirFile = new File( storeDir );
        debug( "Starting " + i + " as current version" );
        switch ( authorativeSlaveId )
        {
        case -1:
            break;
        case -2:
            debug( "At last master starting, deleting store so that it fetches from the new master" );
            FileUtils.deleteRecursively( storeDirFile );
            break;
        default:
            debug( "Consecutive slave starting, making it so that I will copy store from " + authorativeSlaveId );
            FileUtils.deleteRecursively( storeDirFile );
            storeDirFile.mkdirs();
            backup( authorativeSlaveId, storeDirFile );
            break;
        }

        startStandaloneDbToRunUpgrade( storeDirFile, i );

        // start that db up in this JVM
        newDbs[i] = (GraphDatabaseAPI) new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDirFile )
                .setConfig( config( i ) )
                .newGraphDatabase();
        debug( "Started " + i + " as current version" );
        legacyDbs[i] = null;

        // issue transaction and see that it propagates
        if ( i != masterServerId )
        {
            // if the instance is not the old master, create on the old master
            legacyDbs[masterServerId].doComplexLoad( centralNode );
            debug( "Node created on " + i );
        }
        else
        {
            doComplexLoad( newDbs[1], centralNode );
        }
        for ( int j = 0; j < legacyDbs.length; j++ )
        {
            if ( legacyDbs[j] != null )
            {
                legacyDbs[j].verifyComplexLoad( centralNode );
                debug( "Verified on legacy db " + j );
            }
        }
        for ( int j = 0; j < newDbs.length; j++ )
        {
            if ( newDbs[j] != null )
            {
                assertTrue( "Rolled over database " + j + " not available within 1 minute",
                        newDbs[i].isAvailable( MINUTES.toMillis( 1 ) ) );
                verifyComplexLoad( newDbs[j], centralNode );
                debug( "Verified on new db " + j );
            }
        }
    }

    private void startStandaloneDbToRunUpgrade( File storeDir, int dbIndex )
    {
        GraphDatabaseService tempDbForUpgrade = null;
        try
        {
            debug( "Starting standalone db " + dbIndex + " to run upgrade" );
            tempDbForUpgrade = new TestGraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( GraphDatabaseSettings.allow_upgrade, "true" )
                    .newGraphDatabase();
        }
        finally
        {
            if ( tempDbForUpgrade != null )
            {
                tempDbForUpgrade.shutdown();
            }
        }
    }

    private void backup( int sourceServerId, File targetDir ) throws UnknownHostException
    {
        OnlineBackup backup = OnlineBackup.from(localhost(), backupPort(sourceServerId)).backup( targetDir.getPath() );
        assertTrue( "Something wrong with the backup", backup.isConsistent() );
    }

    public void doComplexLoad( GraphDatabaseAPI db, long center )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node central = db.getNodeById( center );

            long type1RelCount = central.getDegree( type1 );
            long type2RelCount = central.getDegree( type2 );

            long[] type1RelId = new long[(int) type1RelCount];
            long[] type2RelId = new long[(int) type2RelCount];

            int index = 0;
            for ( Relationship relationship : central.getRelationships( type1 ) )
            {
                type1RelId[index++] = relationship.getId();
            }
            index = 0;
            for ( Relationship relationship : central.getRelationships( type2 ) )
            {
                type2RelId[index++] = relationship.getId();
            }

            // Delete the first half of each type
            Arrays.sort( type1RelId );
            Arrays.sort( type2RelId );

            for ( int i = 0; i < type1RelId.length / 2; i++ )
            {
                db.getRelationshipById( type1RelId[i] ).delete();
            }
            for ( int i = 0; i < type2RelId.length / 2; i++ )
            {
                db.getRelationshipById( type2RelId[i] ).delete();
            }

            // Go ahead and create relationships to make up for these deletes
            for ( int i = 0; i < type1RelId.length / 2; i++ )
            {
                central.createRelationshipTo( db.createNode(), type1 );
            }

            long largestCreated = 0;
            // The result is the id of the latest created relationship. We'll use that to set the properties
            for ( int i = 0; i < type2RelId.length / 2; i++ )
            {
                long current = central.createRelationshipTo( db.createNode(), type2 ).getId();
                if ( current > largestCreated )
                {
                    largestCreated = current;
                }
            }

            for ( Relationship relationship : central.getRelationships() )
            {
                relationship.setProperty( "relProp", "relProp" + relationship.getId() + "-" + largestCreated );
                Node end = relationship.getEndNode();
                end.setProperty( "nodeProp", "nodeProp" + end.getId() + "-" + largestCreated );
            }

            tx.success();
        }
    }

    public void verifyComplexLoad( GraphDatabaseAPI db, long centralNode ) throws InterruptedException
    {
        db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        try ( Transaction tx = db.beginTx() )
        {
            Node center = db.getNodeById( centralNode );
            long maxRelId = -1;
            for ( Relationship relationship : center.getRelationships() )
            {
                if ( relationship.getId() > maxRelId )
                {
                    maxRelId = relationship.getId();
                }
            }

            int typeCount = 0;
            for ( Relationship relationship : center.getRelationships( type1 ) )
            {
                typeCount++;
                if ( !relationship.getProperty( "relProp" )
                        .equals( "relProp" + relationship.getId() + "-" + maxRelId ) )
                {
                    fail( "damn" );
                }
                Node other = relationship.getEndNode();
                if ( !other.getProperty( "nodeProp" ).equals( "nodeProp" + other.getId() + "-" + maxRelId ) )
                {
                    fail( "double damn" );
                }
            }
            if ( typeCount != 100 )
            {
                fail( "tripled damn" );
            }

            typeCount = 0;
            for ( Relationship relationship : center.getRelationships( type2 ) )
            {
                typeCount++;
                if ( !relationship.getProperty( "relProp" )
                        .equals( "relProp" + relationship.getId() + "-" + maxRelId ) )
                {
                    fail( "damn" );
                }
                Node other = relationship.getEndNode();
                if ( !other.getProperty( "nodeProp" ).equals( "nodeProp" + other.getId() + "-" + maxRelId ) )
                {
                    fail( "double damn" );
                }
            }
            if ( typeCount != 100 )
            {
                fail( "tripled damn" );
            }
            tx.success();
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
}
