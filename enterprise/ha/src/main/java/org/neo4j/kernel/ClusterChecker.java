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
package org.neo4j.kernel;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperMachine;

public class ClusterChecker
{
    private final String clusterName;
    private final ZooKeeperClusterClient clusterClient;

    public ClusterChecker( String clusterName, ZooKeeperClusterClient clusterClient )
    {
        this.clusterName = clusterName;
        this.clusterClient = clusterClient;
    }

    public boolean clusterAlreadyExistsAndThereIsNoMaster()
    {
        clusterClient.waitForSyncConnected();

        try
        {
            Stat clusterNameStat = clusterClient.getZooKeeper( false ).exists( "/" + clusterName, false );

            if ( clusterNameStat == null )
            {
                return false;
            }

            Machine master = clusterClient.getMaster();

            if ( master == ZooKeeperMachine.NO_MACHINE )
            {
                return true;
            }

            return false;
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Unable to connect to ZooKeeper", e );
        }
        catch ( KeeperException e )
        {
            throw new RuntimeException( "There was a problem interacting with ZooKeeper", e );
        }
    }
}
