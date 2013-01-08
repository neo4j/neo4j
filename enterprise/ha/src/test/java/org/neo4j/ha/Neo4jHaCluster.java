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
package org.neo4j.ha;

import java.io.File;
import java.util.Map;

import org.junit.Ignore;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

@Ignore
public class Neo4jHaCluster
{
    public Neo4jHaCluster( LocalhostZooKeeperCluster zk )
    {
    }

    public static HighlyAvailableGraphDatabase single( LocalhostZooKeeperCluster zk, File storeDir,
            int haPort, Map<String, String> config )
    {
        return (HighlyAvailableGraphDatabase) new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( storeDir.getAbsolutePath() ).
            setConfig( HaSettings.server_id, "1" ).
            setConfig( HaSettings.coordinators, zk.getConnectionString() ).
            setConfig( HaSettings.server, "127.0.0.1:" + haPort ).
            setConfig( ShellSettings.remote_shell_enabled, GraphDatabaseSetting.TRUE ).
            setConfig( GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).
            setConfig( config ).
            newGraphDatabase();
    }
}
