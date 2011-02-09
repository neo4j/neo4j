/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.backup;

import static org.neo4j.backup.BackupFromEmbeddedDb.doBackup;
import static org.neo4j.backup.BackupFromEmbeddedDb.getTarget;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.ClusterManager;

/**
 * Get backup from the master of an HA cluster.
 */
public class BackupFromHaCluster
{
    public static void main( String[] args )
    {
        Args arguments = new Args( args );
        String target = getTarget( arguments );
        Number port = arguments.getNumber( "port", null );
        String zooKeeperServers = arguments.get( "from", "localhost:2181" );
        
        // TODO Use clusterName once ClusterManager supports it
//        String clusterName = arguments.get( "cluster", null );
        
        ClusterManager clusterManager = new ClusterManager( zooKeeperServers );
        clusterManager.waitForSyncConnected();
        Pair<String, Integer> masterServer = clusterManager.getMaster().getServer();
        if ( masterServer == null )
        {
            System.out.println( "Unable to connect to HA cluster" );
            System.exit( 1 );
        }
        
        doBackup( target, masterServer.first(), port );
        clusterManager.shutdown();
    }
}
