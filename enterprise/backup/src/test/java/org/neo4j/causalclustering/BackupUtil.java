/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.backup.impl.OnlineBackupCommandBuilder;
import org.neo4j.backup.impl.SelectedBackupProtocol;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.restore.RestoreDatabaseCommand;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.impl.SelectedBackupProtocol.CATCHUP;
import static org.neo4j.causalclustering.BackupCoreIT.backupArguments;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

public class BackupUtil
{
    private static String backupAddress( CoreClusterMember core )
    {
        return core.settingValue( "causal_clustering.transaction_listen_address" );
    }

    public static File createBackupFromCore( CoreClusterMember core, String backupName, File baseBackupDir ) throws Exception
    {
        String[] args = backupArguments( backupAddress( core ), baseBackupDir, backupName );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( baseBackupDir, args ) );
        return new File( baseBackupDir, backupName );
    }

    public static File createBackupInProcess( ClusterMember<?> member, File baseBackupDir, String backupName ) throws Exception
    {
        return createBackupInProcess( member, baseBackupDir, backupName, NULL_OUTPUT_STREAM );
    }

    public static File createBackupInProcess( ClusterMember<?> member, File baseBackupDir, String backupName, OutputStream outputStream ) throws Exception
    {
        AdvertisedSocketAddress address = member.config().get( CausalClusteringSettings.transaction_advertised_address );
        File targetDir = new File( baseBackupDir, backupName );

        new OnlineBackupCommandBuilder()
                .withOutput( outputStream )
                .withSelectedBackupStrategy( CATCHUP )
                .withHost( address.getHostname() )
                .withPort( address.getPort() )
                .backup( targetDir );

        return targetDir;
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, ClusterMember clusterMember ) throws IOException, CommandFailed
    {
        Config config = clusterMember.config();
        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fsa, backup, config, "graph-db", true );
        restoreDatabaseCommand.execute();
    }
}
