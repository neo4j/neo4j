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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import javax.management.remote.JMXServiceURL;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.ConnectionInformation;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.shell.ZooClientFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.management.Neo4jManager;

public class ZooKeeperBroker extends AbstractBroker
{
    private final ZooClientFactory zooClientFactory;
    private volatile ZooClient zooClient;
    private int fetchInfoTimeout;

    public ZooKeeperBroker( Config conf, ZooClientFactory zooClientFactory )
    {
        super( conf );
        this.zooClientFactory = zooClientFactory;
        fetchInfoTimeout = conf.getInteger( HaSettings.coordinator_fetch_info_timeout );
        start();
    }

    @Override
    public void logStatus( StringLogger msgLog )
    {
        for ( String server : zooClient.getServers().split( "," ) )
        {
            msgLog.logMessage( zkStatus( server, "conf" ) );
            msgLog.logMessage( zkStatus( server, "envi" ) );
            msgLog.logMessage( zkStatus( server, "srvr" ) );
        }
    }

    private String zkStatus( String server, String command )
    {
        StringBuilder result = new StringBuilder( "ZooKeeper status: " ).append( server ).append( " " )
                .append( command );
        String[] hostAndPort = server.split( ":" );
        if ( hostAndPort.length != 2 ) return result.append( " BAD SERVER STRING" ).toString();
        String host = hostAndPort[0];
        int port;
        try
        {
            port = Integer.parseInt( hostAndPort[1] );
        }
        catch ( NumberFormatException e )
        {
            return result.append( " BAD SERVER STRING" ).toString();
        }
        SocketAddress sockAddr = new InetSocketAddress( host, port );
        try
        {
            /*
             * There is a chance the zk instance has gone down for the count -
             * the process, the network interface or the whole machine. We don't
             * want to block the main thread in such a case, just fail.
             */
            Socket soc = new Socket();
            soc.connect( sockAddr, fetchInfoTimeout );

            BufferedReader in = new BufferedReader( new InputStreamReader( soc.getInputStream() ) );
            try
            {
                PrintWriter out = new PrintWriter( soc.getOutputStream(), true );
                try
                {
                    out.println( command );
                    for ( String line; ( line = in.readLine() ) != null; )
                    {
                        result.append( "\n  " ).append( line );
                    }
                }
                finally
                {
                    out.close();
                }
            }
            finally
            {
                in.close();
            }
        }
        catch ( Exception e )
        {
            result.append( " FAILED: " + e );
        }
        return result.toString();
    }

    @Override
    public StoreId getClusterStoreId()
    {
        return getZooClient().getClusterStoreId();
    }

    @Override
    public void setConnectionInformation( KernelData kernel )
    {
        String instanceId = kernel.instanceId();
        JMXServiceURL url = Neo4jManager.getConnectionURL(kernel);
        if ( instanceId != null && url != null )
        {
            getZooClient().setJmxConnectionData( url, instanceId );
        }
    }

    @Override
    public ConnectionInformation getConnectionInformation( int machineId )
    {
        for ( ConnectionInformation connection : getConnectionInformation() )
        {
            if ( connection.getMachineId() == machineId ) return connection;
        }
        return null;
    }

    @Override
    public ConnectionInformation[] getConnectionInformation()
    {
        Map<Integer, ZooKeeperMachine> machines = getZooClient().getAllMachines( false );
        Machine master = getZooClient().getMasterBasedOn( machines.values() );
        ConnectionInformation[] result = new ConnectionInformation[machines.size()];
        int i = 0;
        for ( Machine machine : machines.values() )
        {
            result[i++] = addJmxInfo( new ConnectionInformation( machine, master.equals( machine ) ) );
        }
        return result;
    }

    private ConnectionInformation addJmxInfo( ConnectionInformation connect )
    {
        getZooClient().getJmxConnectionData( connect );
        return connect;
    }

    public Pair<Master, Machine> getMaster()
    {
        return getZooClient().getCachedMaster();
    }

    public Pair<Master, Machine> getMasterReally( boolean allowChange )
    {
        return getZooClient().getMasterFromZooKeeper( true, allowChange );
    }

    @Override
    public Machine getMasterExceptMyself()
    {
        Map<Integer, ZooKeeperMachine> machines = getZooClient().getAllMachines( true );
        machines.remove( getMyMachineId() );
        return getZooClient().getMasterBasedOn( machines.values() );
    }

    public Object instantiateMasterServer( GraphDatabaseAPI graphDb )
    {
        return zooClient.instantiateMasterServer( graphDb );
    }

    @Override
    public void setLastCommittedTxId( long txId )
    {
        getZooClient().setCommittedTx( txId );
    }

    public boolean iAmMaster()
    {
        return getZooClient().getCachedMaster().other().getMachineId() == getMyMachineId();
    }

    @Override
    public synchronized void start()
    {
        if ( zooClient != null )
        {
            throw new IllegalStateException(
                    "Broker already started, ZooClient is " + zooClient );
        }
        this.zooClient = zooClientFactory.newZooClient();
    }

    @Override
    public synchronized void shutdown()
    {
        if (zooClient == null)
        {
            throw new IllegalStateException( "Broker already shutdown" );
        }
        zooClient.shutdown();
        zooClient = null;
    }

    @Override
    public synchronized void restart()
    {
        shutdown();
        start();
    }

    @Override
    public void rebindMaster()
    {
        getZooClient().setDataChangeWatcher( ZooClient.MASTER_REBOUND_CHILD,
                getMyMachineId() );
    }

    @Override
    public void notifyMasterChange( Machine newMaster )
    {
        getZooClient().setDataChangeWatcher( ZooClient.MASTER_NOTIFY_CHILD,
                newMaster.getMachineId() );
    }

    protected ZooClient getZooClient()
    {
        if ( zooClient == null )
        {
            throw new IllegalStateException(
                    "This ZooKeeperBroker has been shutdown - no operations are possible until started up again. Maybe the database is restarting?" );
        }
        return zooClient;
    }

    @Override
    public Pair<Master, Machine> bootstrap()
    {
        return getZooClient().bootstrap();
    }
}
