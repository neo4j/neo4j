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
package org.neo4j.com.backup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.com.MasterUtil;
import org.neo4j.com.MasterUtil.TxHandler;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

public class OnlineBackup
{
    private final String hostNameOrIp;
    private final int port;
    private final Map<String, Long> lastCommittedTxs = new TreeMap<String, Long>();

    public static OnlineBackup from( String hostNameOrIp, int port )
    {
        return new OnlineBackup( hostNameOrIp, port );
    }
    
    public static OnlineBackup from( String hostNameOrIp )
    {
        return new OnlineBackup( hostNameOrIp, BackupServer.DEFAULT_PORT );
    }
    
    private OnlineBackup( String hostNameOrIp, int port )
    {
        this.hostNameOrIp = hostNameOrIp;
        this.port = port;
    }
    
    public OnlineBackup full( String targetDirectory )
    {
        if ( directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " already contains a database" );
        }
        
        //                                                     TODO OMG this is ugly
        BackupClient client = new BackupClient( hostNameOrIp, port, new NotYetExistingGraphDatabase( targetDirectory ) );
        try
        {
            Response<Void> response = client.fullBackup( new ToFileStoreWriter( targetDirectory ) );
            GraphDatabaseService targetDb = startTemporaryDb( targetDirectory );
            try
            {
                unpackResponse( response, targetDb, MasterUtil.txHandlerForFullCopy() );
            }
            finally
            {
                targetDb.shutdown();
            }
        }
        finally
        {
            client.shutdown();
            // TODO This is also ugly
            StringLogger.close( targetDirectory + "/messages.log" );
        }
        return this;
    }
    
    private boolean directoryContainsDb( String targetDirectory )
    {
        return new File( targetDirectory, "neostore" ).exists();
    }

    public int getPort()
    {
        return port;
    }
    
    public String getHostNameOrIp()
    {
        return hostNameOrIp;
    }
    
    public Map<String, Long> getLastCommittedTxs()
    {
        return Collections.unmodifiableMap( lastCommittedTxs );
    }

    private EmbeddedGraphDatabase startTemporaryDb( String targetDirectory )
    {
        return new EmbeddedGraphDatabase( targetDirectory );
    }
    
    public OnlineBackup incremental( String targetDirectory )
    {
        if ( !directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " doesn't contain a database" );
        }
        
        GraphDatabaseService targetDb = startTemporaryDb( targetDirectory );
        try
        {
            return incremental( targetDb );
        }
        finally
        {
            targetDb.shutdown();
        }
    }

    public OnlineBackup incremental( GraphDatabaseService targetDb )
    {
        BackupClient client = new BackupClient( hostNameOrIp, port, targetDb );
        try
        {
            unpackResponse( client.incrementalBackup( slaveContextOf( targetDb ) ), targetDb, MasterUtil.NO_ACTION );
        }
        finally
        {
            client.shutdown();
        }
        return this;
    }
    
    private void unpackResponse( Response<Void> response, GraphDatabaseService graphDb, TxHandler txHandler )
    {
        try
        {
            MasterUtil.applyReceivedTransactions( response, graphDb, txHandler );
            getLastCommittedTxs( graphDb );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to apply received transactions", e );
        }
    }
    
    private void getLastCommittedTxs( GraphDatabaseService graphDb )
    {
        for ( XaDataSource ds : ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager().getAllRegisteredDataSources() )
        {
            lastCommittedTxs.put( ds.getName(), ds.getLastCommittedTxId() );
        }
    }

    @SuppressWarnings( "unchecked" )
    private SlaveContext slaveContextOf( GraphDatabaseService graphDb )
    {
        XaDataSourceManager dsManager =
                ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        List<Pair<String, Long>> txs = new ArrayList<Pair<String,Long>>();
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            txs.add( Pair.of( ds.getName(), ds.getLastCommittedTxId() ) ); 
        }
        return new SlaveContext( 0, 0, txs.toArray( new Pair[0] ) );
    }
}
