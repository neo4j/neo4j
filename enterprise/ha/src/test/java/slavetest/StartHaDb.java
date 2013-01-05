/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package slavetest;

import static org.neo4j.shell.impl.AbstractServer.DEFAULT_PORT;

import java.io.File;
import java.io.IOException;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TargetDirectory;

public class StartHaDb
{
    public static final File BASE_PATH = TargetDirectory.forTest( StartHaDb.class ).directory( "dbs", false );

    public static void main( String[] args ) throws Exception
    {
        GraphDatabaseService db = startDb( new Args( args ) );
        System.out.println( "Waiting for ENTER (for clean shutdown)" );
        System.in.read();
        db.shutdown();
    }

    private static GraphDatabaseService startDb( Args args ) throws IOException
    {
        if ( !args.has( "id" ) )
        {
            System.out.println( "Supply 'id=<serverId>'" );
            System.exit( 1 );
        }

        int serverId = args.getNumber( "id", null ).intValue();
        return new HighlyAvailableGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( new File( BASE_PATH,
                "" + serverId ).getAbsolutePath() ).
                setConfig( ClusterSettings.initial_hosts, ":5001,:5002:5003" ).
                setConfig( HaSettings.server_id, "" + serverId ).
                setConfig( HaSettings.ha_server, "127.0.0.1:" + (6001 + serverId) ).
                setConfig( ShellSettings.remote_shell_enabled, GraphDatabaseSetting.TRUE ).
                setConfig( ShellSettings.remote_shell_port, "" + (DEFAULT_PORT + serverId) ).
                setConfig( OnlineBackupSettings.online_backup_enabled, GraphDatabaseSetting.FALSE ).
                newGraphDatabase();
    }
}
