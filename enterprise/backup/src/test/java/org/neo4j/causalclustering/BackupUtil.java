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
package org.neo4j.causalclustering;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.restore.RestoreDatabaseCommand;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.BackupCoreIT.backupArguments;
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

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, ClusterMember clusterMember ) throws IOException, CommandFailed
    {
        Config config = clusterMember.config();
        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fsa, backup, config, "graph-db", true );
        restoreDatabaseCommand.execute();
    }
}
