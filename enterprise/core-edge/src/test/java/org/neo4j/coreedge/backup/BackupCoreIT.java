/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.backup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.coreedge.TestStoreId;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.coreedge.TestStoreId.assertAllStoresHaveTheSameStoreId;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.coreedge.backup.ArgsBuilder.args;
import static org.neo4j.coreedge.backup.ArgsBuilder.toArray;

public class BackupCoreIT
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public ClusterRule clusterRule = new ClusterRule( BackupCoreIT.class )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 )
            .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .withInstanceCoreParam( OnlineBackupSettings.online_backup_server, serverId -> (":" + (8000 + serverId) ) );
    private Cluster cluster;
    private File backupPath;
    private DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Before
    public void setup() throws Exception
    {
        backupPath = clusterRule.testDirectory().cleanDirectory( "backup-db" );
        cluster = clusterRule.startCluster();
    }

    @Test
    public void makeSureBackupCanBePerformed() throws Throwable
    {
        // Run backup
        CoreGraphDatabase db = createSomeData(cluster);
        DbRepresentation beforeChange = DbRepresentation.of( db );
        String[] args = backupArguments(backupAddress(db), backupPath.getPath() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( args ) );

        // Add some new data
        DbRepresentation afterChange = DbRepresentation.of( createSomeData(cluster) );

        // Verify that backed up database can be started and compare representation
        DbRepresentation backupRepresentation = DbRepresentation.of( backupPath, getConfig() );
        assertEquals( beforeChange, backupRepresentation );
        assertNotEquals( backupRepresentation, afterChange );
    }

    @Test
    public void makeSureBackupCanBePerformedFromAnyInstance() throws Throwable
    {
        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            // Run backup
            DbRepresentation beforeChange = DbRepresentation.of(createSomeData( cluster ));
            File backupPathPerCoreMachine = new File( backupPath, "" + db.id().hashCode() );
            String[] args = backupArguments(backupAddress(db.database()), backupPathPerCoreMachine.getPath() );
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( args ) );

            // Add some new data
            DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

            // Verify that old data is back
            DbRepresentation backupRepresentation = DbRepresentation.of( backupPathPerCoreMachine, getConfig() );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }

    @Test
    public void makeSureCoreClusterCanBeRestoredFromABackup() throws Throwable
    {
        // given
        CoreGraphDatabase db = createSomeData( cluster );
        DbRepresentation beforeBackup = DbRepresentation.of( db );
        String[] args = backupArguments(backupAddress(db), backupPath.getPath() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( args ) );

        // when we shutdown the cluster we lose the number of core servers so we won't go through the for loop unless
        // we capture the count beforehand
        List<File> dbPaths = cluster.coreMembers().stream().map( CoreClusterMember::storeDir ).collect( toList() );
        int numberOfCoreMembers = dbPaths.size();

        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( dbPaths, fs );
        StoreId storeId = TestStoreId.readStoreId( dbPaths.get( 0 ), fs );

        // when

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream sysOut = new PrintStream( output );

        Path homeDir = Paths.get(cluster.getCoreMemberById( 0 ).homeDir().getPath());
        new RestoreNewClusterCli( homeDir, homeDir, sysOut ).execute(toArray( args().from( backupPath )
                .database( "graph.db" ).force().build() ));

        String seed = RestoreClusterCliTest.extractSeed( output.toString() );

        for ( int i = 1; i < numberOfCoreMembers; i++ )
        {
            homeDir = Paths.get(cluster.getCoreMemberById( i ).homeDir().getPath());
            new RestoreExistingClusterCli( homeDir, homeDir  ).execute(
                    toArray( args().from( backupPath ).database( "graph.db" ).seed( seed ).force().build() ) );
        }

        cluster.start();

        // then
        Collection<CoreClusterMember> coreGraphDatabases = cluster.coreMembers();
        Stream<DbRepresentation> dbRepresentations = coreGraphDatabases.stream().map( x -> DbRepresentation.of(x.database()) );
        dbRepresentations.forEach( afterReSeed -> assertEquals( beforeBackup, afterReSeed ) );

        List<File> afterRestoreDbPaths = coreGraphDatabases.stream().map( CoreClusterMember::storeDir ).collect( toList() );
        cluster.shutdown();

        assertAllStoresHaveTheSameStoreId( afterRestoreDbPaths, fs );
        StoreId afterRestoreStoreId = TestStoreId.readStoreId( afterRestoreDbPaths.get( 0 ), fs );
        assertNotEquals( storeId, afterRestoreStoreId );
    }

    static CoreGraphDatabase createSomeData( Cluster cluster ) throws TimeoutException, InterruptedException
    {
        return cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } ).database();
    }

    static String backupAddress(CoreGraphDatabase db) {
        InetSocketAddress inetSocketAddress = db.getDependencyResolver()
                .resolveDependency( Config.class ).get( CoreEdgeClusterSettings.transaction_advertised_address )
                .socketAddress();
        return inetSocketAddress.getHostName() + ":" + (inetSocketAddress.getPort() + 2000);
    }

    static String[] backupArguments( String from, String to )
    {
        List<String> args = new ArrayList<>();
        args.add( "-from" );
        args.add( from );
        args.add( "-to" );
        args.add( to );
        return args.toArray( new String[args.size()] );
    }

    static Config getConfig()
    {
        return new Config( MapUtil.stringMap( GraphDatabaseSettings.record_format.name(), StandardV3_0.NAME ) );
    }
}
