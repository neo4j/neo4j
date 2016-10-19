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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.graphdb.Label.label;

public class BackupCoreIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 )
            .withInstanceCoreParam( OnlineBackupSettings.online_backup_server, serverId -> (":" + (8000 + serverId)) );

    private Cluster cluster;
    private File backupsDir;

    @Before
    public void setup() throws Exception
    {
        backupsDir = clusterRule.testDirectory().cleanDirectory( "backups" );
        cluster = clusterRule.startCluster();
    }

    @Test
    public void makeSureBackupCanBePerformedFromAnyInstance() throws Throwable
    {
        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            // Run backup
            DbRepresentation beforeChange = DbRepresentation.of( createSomeData( cluster ) );
            File backupDir = new File( backupsDir, Integer.toString( db.serverId() ) );
            String[] args = backupArguments( backupAddress( db.database() ), backupDir.getPath() );
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( args ) );

            // Add some new data
            DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

            // Verify that old data is back
            DbRepresentation backupRepresentation = DbRepresentation.of( backupDir, getConfig() );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }

    static CoreGraphDatabase createSomeData( Cluster cluster ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } ).database();
    }

    static String backupAddress( GraphDatabaseFacade db )
    {
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
