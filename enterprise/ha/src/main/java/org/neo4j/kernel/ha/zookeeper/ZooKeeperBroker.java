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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.ConnectionInformation;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.management.Neo4jManager;

public class ZooKeeperBroker extends AbstractBroker
{
    // Connect timeout to zk instance for fetching info, in ms
    private static final int FETCH_INFO_TIMEOUT = 500;

    private volatile ZooClient zooClient;
    private final String haServer;
    private int clientLockReadTimeout;
    private final Map<String, String> config;
    private final ResponseReceiver receiver;

    public ZooKeeperBroker( AbstractGraphDatabase graphDb, Map<String, String> config, ResponseReceiver receiver )
    {
        super( HaConfig.getMachineIdFromConfig( config ), graphDb );
        this.config = config;
        haServer = HaConfig.getHaServerFromConfig( config );
        clientLockReadTimeout = HaConfig.getClientLockReadTimeoutFromConfig( config );
        this.receiver = receiver;
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
            soc.connect( sockAddr, FETCH_INFO_TIMEOUT );

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
        return zooClient.getClusterStoreId();
    }

    @Override
    public void setConnectionInformation( KernelData kernel )
    {
        String instanceId = kernel.instanceId();
        JMXServiceURL url = Neo4jManager.getConnectionURL( kernel );
        if ( instanceId != null && url != null )
        {
            zooClient.setJmxConnectionData( url, instanceId );
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
        Map<Integer, ZooKeeperMachine> machines = zooClient.getAllMachines( false );
        Machine master = zooClient.getMasterBasedOn( machines.values() );
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
        zooClient.getJmxConnectionData( connect );
        return connect;
    }

    public Pair<Master, Machine> getMaster()
    {
        return zooClient.getCachedMaster();
    }

    public Pair<Master, Machine> getMasterReally( boolean allowChange )
    {
        return zooClient.getMasterFromZooKeeper( true, allowChange );
    }

    @Override
    public Machine getMasterExceptMyself()
    {
        Map<Integer, ZooKeeperMachine> machines = zooClient.getAllMachines( true );
        machines.remove( getMyMachineId() );
        return zooClient.getMasterBasedOn( machines.values() );
    }

    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        MasterServer server = new MasterServer( new MasterImpl( graphDb, config ),
                Machine.splitIpAndPort( haServer ).other(), getStoreDir(),
                HaConfig.getMaxConcurrentTransactionsOnMasterFromConfig( config ), clientLockReadTimeout,
                new BranchDetectingTxVerifier( (AbstractGraphDatabase)graphDb ) );
        return server;
    }

    @Override
    public void setLastCommittedTxId( long txId )
    {
        zooClient.setCommittedTx( txId );
    }

    public boolean iAmMaster()
    {
        return zooClient.getCachedMaster().other().getMachineId() == getMyMachineId();
    }

    @Override
    public synchronized void start()
    {
        if ( zooClient != null )
        {
            throw new IllegalStateException(
                    "Broker already started, ZooClient is " + zooClient );
        }
        this.zooClient = new ZooClient( getGraphDb(), config, receiver );
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
        zooClient.setDataChangeWatcher( ZooClient.MASTER_REBOUND_CHILD, getMyMachineId() );
    }

    @Override
    public void notifyMasterChange( Machine newMaster )
    {
        zooClient.setDataChangeWatcher( ZooClient.MASTER_NOTIFY_CHILD, newMaster.getMachineId() );
    }

    protected ZooClient getZooClient()
    {
        return zooClient;
    }
}
