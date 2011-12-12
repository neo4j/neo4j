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
package org.neo4j.kernel.ha.zookeeper;

import static org.neo4j.com.Server.DEFAULT_BACKUP_PORT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.com.Client;

public class ClusterManager extends AbstractZooKeeperManager
{
    private final ZooKeeper zooKeeper;
    private String rootPath;
    private KeeperState state = KeeperState.Disconnected;
    
    public ClusterManager( String zooKeeperServers )
    {
        super( zooKeeperServers, null, Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT );
        this.zooKeeper = instantiateZooKeeper();
    }
    
    @Override
    protected int getMyMachineId()
    {
        throw new UnsupportedOperationException("Not implemented ClusterManager.getMyMachineId()");
    }
    
    public void waitForSyncConnected()
    {
        long startTime = System.currentTimeMillis();
        while ( System.currentTimeMillis()-startTime < SESSION_TIME_OUT )
        {
            if ( state == KeeperState.SyncConnected )
            {
                // We are connected
                break;
            }
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }
    
    public int getBackupPort( int machineId )
    {
        int port = readHaServer( machineId, true ).other();
        return port != 0 ? port : DEFAULT_BACKUP_PORT;
    }

    public void process( WatchedEvent event )
    {
        // System.out.println( "Got event: " + event );
        String path = event.getPath();
        if ( path == null )
        {
            state = event.getState();
        }
    }
    
    public Machine getMaster()
    {
        return getMasterBasedOn( getAllMachines( true ).values() );
    }
    
    @Override
    public String getRoot()
    {
        if ( rootPath == null )
        {
            rootPath = readRootPath();
        }
        return rootPath;
    }
    
    private String readRootPath()
    {
        waitForSyncConnected();
        String result = getSingleRootPath( getZooKeeper() );
        if ( result == null )
        {
            throw new RuntimeException( "No root child found in zoo keeper" );
        }
        return result;
    }

    public static String getSingleRootPath( ZooKeeper keeper )
    {
        try
        {
            List<String> children = keeper.getChildren( "/", false );
            String foundChild = null;
            for ( String child : children )
            {
                if ( child.contains( "_" ) )
                {
                    if ( foundChild != null )
                    {
                        throw new RuntimeException( "Multiple roots found, " +
                                foundChild + " and " + child );
                    }
                    foundChild = child;
                }
            }
        
            if ( foundChild != null )
            {
                return "/" + foundChild;
            }
            return null;
        }
        catch ( KeeperException e )
        {
            throw new RuntimeException( e );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Returns the disconnected slaves in this cluster so that all slaves
     * which are specified in the HA servers configuration, but not in the
     * zoo keeper cluster will be returned.
     * @return the disconnected slaves in this cluster.
     */
//    public Machine[] getDisconnectedSlaves()
//    {
//        Collection<Machine> infos = new ArrayList<Machine>();
//        for ( Map.Entry<Integer, String> entry : haServers.entrySet() )
//        {
//            infos.add( new Machine( entry.getKey(), -1, -1, entry.getValue() ) );
//        }
//        infos.removeAll( getAllMachines().values() );
//        return infos.toArray( new Machine[infos.size()] );
//    }
    
    /**
     * Returns the connected slaves in this cluster.
     * @return the connected slaves in this cluster.
     */
    public Machine[] getConnectedSlaves()
    {
        Map<Integer, Machine> machines = getAllMachines( true );
        Machine master = getMasterBasedOn( machines.values() );
        Collection<Machine> result = new ArrayList<Machine>( machines.values() );
        result.remove( master );
        return result.toArray( new Machine[result.size()] );
    }
    
    @Override
    protected ZooKeeper getZooKeeper()
    {
        return this.zooKeeper;
    }
}
