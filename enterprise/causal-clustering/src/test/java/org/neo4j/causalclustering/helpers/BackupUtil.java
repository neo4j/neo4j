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
package org.neo4j.causalclustering.helpers;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.restore.RestoreDatabaseCommand;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.OnlineBackupCommandIT.runBackupToolFromOtherJvmToGetExitCode;

public class BackupUtil
{
    public static String backupAddress( GraphDatabaseFacade db )
    {
        InetSocketAddress inetSocketAddress = db.getDependencyResolver()
                .resolveDependency( Config.class ).get( CausalClusteringSettings.transaction_advertised_address )
                .socketAddress();

        HostnamePort hostnamePort = db.getDependencyResolver()
            .resolveDependency( Config.class )
            .get( OnlineBackupSettings.online_backup_server );

        return inetSocketAddress.getHostName() + ":" + hostnamePort.getPort();
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
        Map<String,String> config = MapUtil.stringMap(
                GraphDatabaseSettings.record_format.name(), Standard.LATEST_NAME,
                OnlineBackupSettings.online_backup_server.name(), "127.0.0.1:" + PortAuthority.allocatePort()
        );

        return Config.defaults( config );
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
