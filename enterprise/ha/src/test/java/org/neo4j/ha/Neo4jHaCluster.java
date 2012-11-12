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
package org.neo4j.ha;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
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
        config = new HashMap<String, String>( config );
        config.put( HaConfig.CONFIG_KEY_SERVER_ID, "1" );
        config.put( HaConfig.CONFIG_KEY_COORDINATORS, zk.getConnectionString() );
        config.put( HaConfig.CONFIG_KEY_SERVER, ( "127.0.0.1:" + haPort ) );
        config.put( Config.ENABLE_REMOTE_SHELL, "true" );
        config.put( Config.KEEP_LOGICAL_LOGS, "true" );
        return new HighlyAvailableGraphDatabase( storeDir.getAbsolutePath(), config );
    }
}
