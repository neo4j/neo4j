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

import org.neo4j.com.Server;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

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
        return new HighlyAvailableGraphDatabase( PATH.getPath(), MapUtil.stringMap(
                HaConfig.CONFIG_KEY_SERVER_ID, "2",
                HaConfig.CONFIG_KEY_COORDINATORS, "127.0.0.1:2181,127.0.0.1:2182",
                HaConfig.CONFIG_KEY_SERVER, ME,
                Config.ENABLE_REMOTE_SHELL, "port=1338",
                Config.KEEP_LOGICAL_LOGS, "true",
                Config.ENABLE_ONLINE_BACKUP, "port=" + (Server.DEFAULT_BACKUP_PORT+1) ) );
    }
}
