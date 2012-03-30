/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package slavetest.manual;

import java.io.File;
import java.io.IOException;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.com.Server;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.shell.ShellSettings;

public class ManualTest2
{
    public static final File PATH = new File( "var/man/db2" );
    static final String ME = "127.0.0.1:5560";
        
    public static void main( String[] args ) throws Exception
    {
        GraphDatabaseService db = startDb();
        System.out.println( "Waiting for ENTER (for clean shutdown)" );
        System.in.read();
        db.shutdown();
    }

    private static GraphDatabaseService startDb() throws IOException
    {
        return new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( PATH.getPath() ).
                setConfig( HaSettings.server_id, "2" ).
                setConfig( HaSettings.coordinators, "127.0.0.1:2181,127.0.0.1:2182" ).
                setConfig( HaSettings.server, ME ).
                setConfig( ShellSettings.remote_shell_enabled, GraphDatabaseSetting.TRUE ).
                setConfig( ShellSettings.remote_shell_port, "1338" ).
                setConfig( GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).
                setConfig( OnlineBackupSettings.online_backup_enabled, GraphDatabaseSetting.TRUE ).
                setConfig( OnlineBackupSettings.online_backup_port, ""+Server.DEFAULT_BACKUP_PORT+1 ).
                newGraphDatabase();
    }
}
