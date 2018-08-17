/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.causalclustering.ClusterHelper.createSomeData;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

public class BackupHaIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .withInstanceSetting( OnlineBackupSettings.online_backup_server, serverId -> ":" + PortAuthority.allocatePort() );
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private File backupPath;

    @Before
    public void setup() throws Exception
    {
        backupPath = clusterRule.getTestDirectory().storeDir( "backup-db" );
        createSomeData( clusterRule.startCluster().getMaster() );
    }

    @Test
    public void makeSureBackupCanBePerformed() throws Throwable
    {
        // Run backup
        ManagedCluster cluster = clusterRule.startCluster();
        DbRepresentation beforeChange = DbRepresentation.of( cluster.getMaster() );
        HighlyAvailableGraphDatabase hagdb = cluster.getAllMembers().iterator().next();
        HostnamePort address = cluster.getBackupAddress(hagdb);
        String databaseName = "basic";
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupPath, backupArguments( address.toString(),
                backupPath, databaseName ) ) );

        // Add some new data
        DbRepresentation afterChange = createSomeData( cluster.getMaster() );
        cluster.sync();

        // Verify that backed up database can be started and compare representation
        Config config = Config.builder()
                .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .withSetting( GraphDatabaseSettings.active_database, databaseName ).build();
        DbRepresentation backupRepresentation = DbRepresentation.of( DatabaseLayout.of( backupPath, databaseName ).databaseDirectory(), config );
        assertEquals( beforeChange, backupRepresentation );
        assertNotEquals( backupRepresentation, afterChange );
    }

    @Test
    public void makeSureBackupCanBePerformedFromAnyInstance() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();

        for ( HighlyAvailableGraphDatabase hagdb : cluster.getAllMembers() )
        {
            HostnamePort address = cluster.getBackupAddress(hagdb);

            // Run backup
            DbRepresentation beforeChange = DbRepresentation.of( cluster.getMaster() );
            String databaseName = "anyinstance";
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupPath, backupArguments( address.toString(),
                    backupPath, databaseName ) ) );

            // Add some new data
            DbRepresentation afterChange = createSomeData( cluster.getMaster() );
            cluster.sync();

            // Verify that old data is back
            Config config = Config.builder()
                    .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                    .withSetting( GraphDatabaseSettings.active_database, databaseName ).build();
            DbRepresentation backupRepresentation = DbRepresentation.of( DatabaseLayout.of( backupPath, databaseName ).databaseDirectory(), config );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }

    private static String[] backupArguments( String from, File backupDir, String databaseName )
    {
        List<String> args = new ArrayList<>();
        args.add( "--from=" + from );
        args.add( "--cc-report-dir=" + backupDir );
        args.add( "--backup-dir=" + backupDir );
        args.add( "--name=" + databaseName );
        return args.toArray( new String[args.size()] );
    }
}
