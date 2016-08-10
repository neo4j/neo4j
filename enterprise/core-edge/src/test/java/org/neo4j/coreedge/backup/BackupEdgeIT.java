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

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.coreedge.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.coreedge.backup.BackupCoreIT.backupArguments;

public class BackupEdgeIT
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppress( SuppressOutput.System.out );

    @Rule
    public ClusterRule clusterRule = new ClusterRule( BackupCoreIT.class )
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withNumberOfEdgeMembers( 1 )
            .withSharedEdgeParam( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .withInstanceEdgeParam( OnlineBackupSettings.online_backup_server, serverId -> ":8000" );

    private File backupPath;

    @Before
    public void setup() throws Exception
    {
        backupPath = clusterRule.testDirectory().cleanDirectory( "backup-db" );
        clusterRule.startCluster();
    }

    @Test
    public void makeSureBackupCannotBePerformed() throws Throwable
    {
        String[] args = backupArguments( "localhost:8000", backupPath.getPath(), false );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( args ) );
    }
}
