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

package slavetest;

import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;

public class DumpZooInfo
{
    public static void main( String[] args )
    {
        Args arguments = new Args( args );
        ZooKeeperClusterClient clusterManager = new ZooKeeperClusterClient( "localhost", arguments.get( HaSettings.cluster_name.name(), ConfigurationDefaults.getDefault( HaSettings.cluster_name, HaSettings.class ) ));
        clusterManager.waitForSyncConnected();
        System.out.println( "Master is " + clusterManager.getCachedMaster() );
        System.out.println( "Connected slaves" );
        for ( Machine info : clusterManager.getConnectedSlaves() )
        {
            System.out.println( "\t" + info );
        }
//        System.out.println( "Disconnected slaves" );
//        for ( Machine info : clusterManager.getDisconnectedSlaves() )
//        {
//            System.out.println( "\t" + info );
//        }
        clusterManager.shutdown();
    }
}
