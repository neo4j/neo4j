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
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.onlinebackup.net.Callback;
import org.neo4j.onlinebackup.net.ConnectToMasterJob;
import org.neo4j.onlinebackup.net.Connection;
import org.neo4j.onlinebackup.net.Job;
import org.neo4j.onlinebackup.net.JobEater;

public abstract class AbstractSlave implements Callback
{
    private final EmbeddedReadOnlyGraphDatabase graphDb;
//    private final NeoStoreXaDataSource xaDs;
//    private final XaDataSource luceneXaDs;
//    private final XaDataSource luceneFulltextXaDs;
    
    private final XaDataSource[] xaDataSources;
    private final Connection[] masterConnections;
    
    private final JobEater jobEater;
    private final LogApplier logApplier;
    
    private final String masterIp;
    private final int masterPort;
//    private Connection masterConnection;
//    private Connection luceneConnection;
//    private Connection luceneFulltextConnection;
    
    public AbstractSlave( String path, Map<String,String> params, 
        String masterIp, int masterPort )
    {
        params.put( "backup_slave", "true" );
        this.graphDb = new EmbeddedReadOnlyGraphDatabase( path, params );
        XaDataSourceManager xaDsMgr = graphDb.getConfig().getTxModule().
            getXaDataSourceManager();
        XaDataSource nioneo = xaDsMgr.getXaDataSource( "nioneodb" );
        XaDataSource lucene = xaDsMgr.getXaDataSource( "lucene" );
        XaDataSource fulltext = xaDsMgr.getXaDataSource( "lucene-fulltext" );
        if ( lucene != null && fulltext != null )
        {
            xaDataSources = new XaDataSource[3];
            xaDataSources[0] = nioneo;
            xaDataSources[1] = lucene;
            xaDataSources[2] = fulltext;
        }
        else
        {
            xaDataSources = new XaDataSource[1];
            xaDataSources[0] = nioneo;
        }
        for ( XaDataSource xaDs : xaDataSources )
        {
            xaDs.makeBackupSlave();
        }
        recover();

        jobEater = new JobEater();
        logApplier = new LogApplier( xaDataSources );
        jobEater.start();
        logApplier.start();
        
        this.masterIp = masterIp;
        this.masterPort = masterPort;
        masterConnections = new Connection[xaDataSources.length];
        for ( int i = 0; i < masterConnections.length; i++ )
        {
            masterConnections[i] = new Connection( masterIp, masterPort );
            while ( !masterConnections[i].connected() )
            {
                if ( masterConnections[i].connectionRefused() )
                {
                    System.out.println( "Unable to connect to master" );
                    break;
                }
            }
            if ( masterConnections[i].connected() )
            {
                String name = "nioneodb";
                if ( i == 1 )
                {
                    name = "lucene";
                }
                else if ( i == 2 )
                {
                    name = "lucene-fulltext";
                }
                jobEater.addJob( new ConnectToMasterJob( masterConnections[i], 
                        this, name, xaDataSources[i] ) );
            }
        }
//        System.out.println( "At version: " + getVersion() );
    }
    
    private void recover()
    {
        for ( XaDataSource xaDs : xaDataSources )
        {
            long nextVersion = xaDs.getCurrentLogVersion();
            while ( xaDs.hasLogicalLog( nextVersion ) )
            {
                try
                {
                    xaDs.applyLog( xaDs.getLogicalLog( nextVersion ) );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( 
                        "Unable to recover slave to consistent state", e );
                }
                nextVersion++;
            }
        }
    }
    
    public boolean isConnectedToMaster()
    {
        for ( Connection masterConnection : masterConnections )
        {
            if ( !masterConnection.connected() )
            {
                return false;
            }
        }
        return true;
    }
    
    public boolean reconnectToMaster()
    {
        for ( int i = 0; i < masterConnections.length; i++ )
        {
            if ( masterConnections[i].connected() )
            {
                continue;
            }
            masterConnections[i] = new Connection( masterIp, masterPort );
            while ( !masterConnections[i].connected() )
            {
                if ( masterConnections[i].connectionRefused() )
                {
                    return false;
                }
            }
            if ( masterConnections[i].connected() )
            {
                String name = "nioneodb";
                if ( i == 1 )
                {
                    name = "lucene";
                }
                else if ( i == 2 )
                {
                    name = "lucene-fulltext";
                }
                jobEater.addJob( new ConnectToMasterJob( masterConnections[i], 
                        this, name, xaDataSources[i] ) );
            }
        }
        return true;
    }
    
    public String getMasterIp()
    {
        return masterIp;
    }
    
    public int getMasterPort()
    {
        return masterPort;
    }
    
    public void jobExecuted( Job job )
    {
    }
    
//    public long getIdentifier()
//    {
//        return xaDs.getRandomIdentifier();
//    }
//    
//    public long getCreationTime()
//    {
//        return xaDs.getCreationTime();
//    }
//    
//    public long getVersion()
//    {
//        return xaDs.getCurrentLogVersion();
//    }
//
//    public boolean hasLog( long version )
//    {
//        return xaDs.hasLogicalLog( version );
//    }
//
//    public String getLogName( long version )
//    {
//        return xaDs.getFileName( version );
//    }

    public void shutdown()
    {
        jobEater.stopEating();
        logApplier.stopApplyLogs();
        graphDb.shutdown();
    }
    
    protected GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }
}
