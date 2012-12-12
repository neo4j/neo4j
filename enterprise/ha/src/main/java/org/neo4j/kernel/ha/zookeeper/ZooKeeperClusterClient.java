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

package org.neo4j.kernel.ha.zookeeper;

import static org.neo4j.com.Protocol.DEFAULT_FRAME_LENGTH;
import static org.neo4j.com.Server.DEFAULT_BACKUP_PORT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.com.Client;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClientFactory;
import org.neo4j.kernel.ha.MasterClientResolver;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

public class ZooKeeperClusterClient extends AbstractZooKeeperManager implements ClusterClient
{
    protected static final int SESSION_TIME_OUT = 5000;
    public static final int CLUSTER_CLIENT_MACHINE_ID = -1;

    private final ZooKeeper zooKeeper;
    private String rootPath;
    private KeeperState state = KeeperState.Disconnected;
    private final String clusterName;

    public ZooKeeperClusterClient( String zooKeeperServers )
    {
        this( zooKeeperServers, ConfigurationDefaults.getDefault( HaSettings.cluster_name, HaSettings.class));
    }

    public ZooKeeperClusterClient( String zooKeeperServers, String clusterName )
    {
        this( zooKeeperServers, StringLogger.SYSTEM, clusterName, Integer.parseInt( ConfigurationDefaults.getDefault(
                HaSettings.zk_session_timeout, HaSettings.class ) ), new MasterClientResolver.F18( StringLogger.SYSTEM,
                Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS, Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT, DEFAULT_FRAME_LENGTH ) );
    }

    public ZooKeeperClusterClient( String zooKeeperServers, StringLogger msgLog, String clusterName,
            int sessionTimeout, MasterClientFactory factory )
    {
        super( zooKeeperServers, msgLog, sessionTimeout, factory );
        this.clusterName = clusterName;
        try
        {
            zooKeeper = createZooKeeper( zooKeeperServers );
        }
        catch ( IOException e )
        {
            throw new ZooKeeperException(
                "Unable to create zoo keeper client", e );
        }
    }

    protected ZooKeeper createZooKeeper( String zooKeeperServers ) throws IOException
    {
        return new ZooKeeper( zooKeeperServers, SESSION_TIME_OUT, new WatcherImpl() );
    }

    @Override
    void waitForSyncConnected( WaitMode waitMode )
    {
        long startTime = System.currentTimeMillis();
        while ( System.currentTimeMillis() - startTime < getSessionTimeout() )
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

    @Override
    protected int getMyMachineId()
    {
        return CLUSTER_CLIENT_MACHINE_ID;
    }

    public int getBackupPort( int machineId )
    {
        int port = readHaServer( machineId, true ).getBackupPort();
        return port != 0 ? port : DEFAULT_BACKUP_PORT;
    }

    public Machine getMaster()
    {
        if ( readRootPath() == null )
        {
            return null;
        }
        return getMasterBasedOn( getAllMachines( true ).values() );
    }

    public Pair<Master, Machine> getMasterClient()
    {
        Machine masterMachine = getMaster();
        if ( masterMachine == null )
        {
            return null;
        }
        Master masterClient = getMasterClientToMachine( masterMachine );
        return Pair.of( masterClient, masterMachine );
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
        StoreId storeId = getClusterStoreId( zooKeeper, clusterName );
        if( storeId == null )
        {
            throw new RuntimeException( "Cluster '" + clusterName + "' not found" );
        }
        return asRootPath( storeId );
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
        Map<Integer, ZooKeeperMachine> machines = getAllMachines( true );
        Machine master = getMasterBasedOn( machines.values() );
        Collection<Machine> result = new ArrayList<Machine>( machines.values() );
        result.remove( master );
        return result.toArray( new Machine[result.size()] );
    }

    @Override
    public ZooKeeper getZooKeeper( boolean sync )
    {
        if( sync )
        {
            this.zooKeeper.sync( getRoot(), null, null );
        }
        return this.zooKeeper;
    }

    private class WatcherImpl
        implements Watcher
    {
        public void process( WatchedEvent event )
        {
            // System.out.println( "Got event: " + event );
            String path = event.getPath();
            if ( path == null )
            {
                state = event.getState();
            }
        }
    }
}
