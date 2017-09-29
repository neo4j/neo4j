/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.backup.OnlineBackupCommandCcIT.createSomeData;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

public class BackupHaIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .withInstanceSetting( OnlineBackupSettings.online_backup_server, serverId -> ":" + PortAuthority.allocatePort() );
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private File backupPath;

    @Before
    public void setup() throws Exception
    {
        backupPath = clusterRule.cleanDirectory( "backup-db" );
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
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupPath, backupArguments( address.toString(),
                backupPath, "basic" ) ) );

        // Add some new data
        DbRepresentation afterChange = createSomeData( cluster.getMaster() );
        cluster.sync();

        // Verify that backed up database can be started and compare representation
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        DbRepresentation backupRepresentation = DbRepresentation.of( new File( backupPath, "basic" ), config );
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
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupPath, backupArguments( address.toString(),
                    backupPath, "anyinstance" ) ) );

            // Add some new data
            DbRepresentation afterChange = createSomeData( cluster.getMaster() );
            cluster.sync();

            // Verify that old data is back
            Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
            DbRepresentation backupRepresentation = DbRepresentation.of( new File( backupPath, "anyinstance" ), config );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }

    private String[] backupArguments( String from, File backupDir, String name )
    {
        List<String> args = new ArrayList<>();
        args.add( "--from=" + from );
        args.add( "--cc-report-dir=" + backupDir );
        args.add( "--backup-dir=" + backupDir );
        args.add( "--name=" + name );
        return args.toArray( new String[args.size()] );
    }
}
