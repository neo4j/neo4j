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
package org.neo4j.causalclustering.helpers;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.backup.BackupCoreIT;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.restore.RestoreDatabaseCommand;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.OnlineBackupCommandIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.graphdb.Label.label;

public class BackupUtil
{
    public static String backupAddress( GraphDatabaseFacade db )
    {
        InetSocketAddress inetSocketAddress = db.getDependencyResolver()
                .resolveDependency( Config.class ).get( CausalClusteringSettings.transaction_advertised_address )
                .socketAddress();
        return inetSocketAddress.getHostName() + ":" + (inetSocketAddress.getPort() + 2000);
    }

    public static String[] backupArguments( String from, File backupsDir, String name )
    {
        List<String> args = new ArrayList<>();
        args.add( "--from=" + from );
        args.add( "--cc-report-dir=" + backupsDir );
        args.add( "--backup-dir=" + backupsDir );
        args.add( "--name=" + name );
        return args.toArray( new String[args.size()] );
    }

    public static Config getConfig()
    {
        return Config.embeddedDefaults( MapUtil.stringMap( GraphDatabaseSettings.record_format.name(), Standard.LATEST_NAME ) );
    }

    public static File createBackupFromCore( CoreGraphDatabase db, String backupName, File baseBackupDir ) throws Exception
    {
        String[] args = backupArguments( backupAddress( db ), baseBackupDir, backupName );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( baseBackupDir, args ) );
        return new File( baseBackupDir, backupName );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, CoreClusterMember coreClusterMember )
            throws IOException, CommandFailed
    {
        Config config = coreClusterMember.config();
        RestoreDatabaseCommand restoreDatabaseCommand =
                new RestoreDatabaseCommand( fsa, backup, config, "graph-db", true );
        restoreDatabaseCommand.execute();
    }
}
