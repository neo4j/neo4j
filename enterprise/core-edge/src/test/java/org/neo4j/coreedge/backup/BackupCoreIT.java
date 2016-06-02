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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.coreedge.convert.ConvertClassicStoreCommand;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.restore.RestoreDatabaseCommand;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.rule.SuppressOutput.suppress;

public class BackupCoreIT
{
    @Rule
    public SuppressOutput suppressOutput = suppress( SuppressOutput.System.out, SuppressOutput.System.err );

    @Rule
    public ClusterRule clusterRule = new ClusterRule( BackupCoreIT.class )
            .withNumberOfCoreServers( 3 )
            .withNumberOfEdgeServers( 0 )
            .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .withInstanceCoreParam( OnlineBackupSettings.online_backup_server, serverId -> (":" + (8000 + serverId) ) );
    private Cluster cluster;
    private File backupPath;

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
        for ( CoreGraphDatabase db : cluster.coreServers() )
        {
            // Run backup
            DbRepresentation beforeChange = DbRepresentation.of(createSomeData( cluster ));
            File backupPathPerCoreMachine = new File( backupPath, "" + db.id().hashCode() );
            String[] args = backupArguments(backupAddress(db), backupPathPerCoreMachine.getPath() );
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
        int numberOfCoreServers = cluster.coreServers().size();

        cluster.shutdown();

        // when
        for ( int i = 0; i < numberOfCoreServers; i++ )
        {
            File coreStoreDir = cluster.coreServerStoreDirectory( i );
            restoreDatabase( coreStoreDir, backupPath );
            ConvertClassicStoreCommand convertCommand = new ConvertClassicStoreCommand( coreStoreDir, StandardV3_0.NAME );
            convertCommand.execute();
        }

        cluster.start();

        // then
        Stream<DbRepresentation> dbRepresentations = cluster.coreServers().stream().map( DbRepresentation::of );
        dbRepresentations.forEach( afterReSeed -> assertEquals( beforeBackup, afterReSeed ) );
    }

    private void restoreDatabase( File coreStoreDir, File backupPath ) throws IOException
    {
        Config config = Config.empty().with(
                stringMap( DatabaseManagementSystemSettings.database_path.name(), coreStoreDir.getAbsolutePath() ) );
        new RestoreDatabaseCommand( new DefaultFileSystemAbstraction(),
                backupPath, config, "graph.db", true ).execute();
    }

    static CoreGraphDatabase createSomeData( Cluster cluster ) throws TimeoutException, InterruptedException
    {
        return cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );
    }

    static String backupAddress(CoreGraphDatabase db) {
        InetSocketAddress inetSocketAddress = db.id().getCoreAddress().socketAddress();
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
