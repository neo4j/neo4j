/*
 * Copyright (c) 2009-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.onlinebackup.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.onlinebackup.net.AcceptJob;
import org.neo4j.onlinebackup.net.Callback;
import org.neo4j.onlinebackup.net.Connection;
import org.neo4j.onlinebackup.net.HandleIncommingSlaveJob;
import org.neo4j.onlinebackup.net.HandleSlaveConnection;
import org.neo4j.onlinebackup.net.Job;
import org.neo4j.onlinebackup.net.JobEater;
import org.neo4j.onlinebackup.net.SocketException;

public class Master implements Callback
{
    private final EmbeddedGraphDatabase graphDb;
    private final XaDataSourceManager xaDsMgr;

    private final JobEater jobEater;
    private final ServerSocketChannel serverChannel;
    private final int port;
    
    private List<HandleSlaveConnection> slaveList = new 
        CopyOnWriteArrayList<HandleSlaveConnection>();
    
    public Master( String path, Map<String,String> params, int listenPort )
    {
        this.graphDb = new EmbeddedGraphDatabase( path, params );
        this.xaDsMgr = graphDb.getConfig().getTxModule()
            .getXaDataSourceManager();
        for ( XaDataSource xaDs : xaDsMgr.getAllRegisteredDataSources() )
        {
            xaDs.keepLogicalLogs( true );
        }
        this.port = listenPort;
        try
        {
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking( false );
            serverChannel.socket().bind( new InetSocketAddress( listenPort ) );
            
        }
        catch ( IOException e )
        {
            throw new SocketException( "Unable to bind at port[" + 
                listenPort + "]", e );
        }
        jobEater = new JobEater();
        jobEater.addJob( new AcceptJob( this, serverChannel ) );
        jobEater.start();
    }
    
    public GraphDatabaseService getGraphDbService()
    {
        return graphDb;
    }
    
    public int getPort()
    {
        return this.port;
    }
    
    public void jobExecuted( Job job )
    {
        if ( job instanceof AcceptJob )
        {
            // handle incomming slave
            AcceptJob acceptJob = (AcceptJob) job;
            if ( acceptJob.getAcceptedChannel() != null )
            {
                Connection connection = new Connection( 
                    ((AcceptJob) job).getAcceptedChannel() );
                jobEater.addJob( 
                    new HandleIncommingSlaveJob( connection, this ) );
            }
        }
        else if ( job instanceof HandleIncommingSlaveJob )
        {
            HandleSlaveConnection chainJob = 
                (HandleSlaveConnection) job.getChainJob();
            if ( chainJob != null )
            {
                slaveList.add( chainJob );
            }
            else
            {
                System.out.println( "null chain job" );
            }
        }
    }
    
    public void shutdown()
    {
        jobEater.stopEating();
        try
        {
            serverChannel.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        graphDb.shutdown();
    }
    
    public long getIdentifier( String xaDsName  )
    {
        XaDataSource xaDs = xaDsMgr.getXaDataSource( xaDsName );
        if ( xaDs != null )
        {
            return xaDs.getRandomIdentifier();
        }
        return -1;
    }
    
    public long getCreationTime( String xaDsName )
    {
        XaDataSource xaDs = xaDsMgr.getXaDataSource( xaDsName );
        if ( xaDs != null )
        {
            return xaDs.getCreationTime();
        }
        return -1;
    }
    
    public long getVersion( String xaDsName )
    {
        XaDataSource xaDs = xaDsMgr.getXaDataSource( xaDsName );
        if ( xaDs != null )
        {
            return xaDs.getCurrentLogVersion();
        }
        return -1;
    }
    
    public ReadableByteChannel getLog( String xaDsName, long version ) 
        throws IOException
    {
        XaDataSource xaDs = xaDsMgr.getXaDataSource( xaDsName );
        if ( xaDs != null )
        {
            return xaDs.getLogicalLog( version );
        }
        return null;
    }
    
    public long getLogLength( String xaDsName, long version )
    {
        XaDataSource xaDs = xaDsMgr.getXaDataSource( xaDsName );
        if ( xaDs != null )
        {
            return xaDs.getLogicalLogLength( version );
        }
        return -1l;
    }
    
    public boolean hasLog( String xaDsName, long version )
    {
        XaDataSource xaDs = xaDsMgr.getXaDataSource( xaDsName );
        if ( xaDs != null )
        {
            return xaDs.hasLogicalLog( version );
        }
        return false;
    }

    public synchronized void rotateLogAndPushToSlaves() throws IOException
    {
        if ( slaveList.size() == 0 )
        {
            return;
        }
        for ( XaDataSource xaDs : xaDsMgr.getAllRegisteredDataSources() )
        {
            xaDs.rotateLogicalLog();
        }
        List<HandleSlaveConnection> newList = 
            new CopyOnWriteArrayList<HandleSlaveConnection>();
        for ( HandleSlaveConnection slave : slaveList )
        {
            XaDataSource xaDs = xaDsMgr.getXaDataSource( slave.getXaDsName() );
            if ( xaDs != null )
            {
                long version = xaDs.getCurrentLogVersion() - 1;
                if ( !slave.offerLogToSlave( version ) )
                {
                    System.out.println( "Failed to offer log to slave: " + slave );
                }
                else
                {
                    newList.add( slave );
                }
            }
        }
        slaveList = newList;
    }
}