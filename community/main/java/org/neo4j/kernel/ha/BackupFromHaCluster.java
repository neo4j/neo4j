/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import java.util.Map;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class BackupFromHaCluster
{
    public static void main( String[] args ) throws Exception
    {
        Args arguments = new Args( args );
        String storeDir = arguments.get( "path",
                !arguments.orphans().isEmpty() ? arguments.orphans().get( 0 ) : null );
        ClusterManager cluster = new ClusterManager( arguments.get(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, null ) );
        cluster.waitForSyncConnected();
        
        final Machine master = cluster.getCachedMaster().other();
        System.out.println( "Master:" + master );
        Map<String, String> config = MapUtil.stringMap(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID,
                String.valueOf( master.getMachineId() ) );
        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( storeDir, config,
                AbstractBroker.wrapSingleBroker( new BackupBroker( new MasterClient(
                        master.getServer().first(), master.getServer().other(), storeDir ), storeDir ) ) );
        System.out.println( "Leaching backup from master " + master );
        try
        {
            db.pullUpdates();
            System.out.println( "Backup completed successfully" );
        }
        finally
        {
            db.shutdown();
            cluster.shutdown();
        }
    }
}
