package org.neo4j.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.com.MasterUtil;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.com.MasterUtil.TxHandler;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class OnlineBackup
{
    private final BackupClient client;

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
        this.client = new BackupClient( hostNameOrIp, port, null );
    }
    
    public OnlineBackup full( String targetDirectory )
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
        return this;
    }

    private EmbeddedGraphDatabase startTemporaryDb( String targetDirectory )
    {
        // TODO Constant
        return new EmbeddedGraphDatabase( targetDirectory, MapUtil.stringMap( "enable_online_backup", "false" ) );
    }
    
    public OnlineBackup incremental( String targetDirectory )
    {
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
        unpackResponse( client.incrementalBackup( slaveContextOf( targetDb ) ), targetDb, MasterUtil.NO_ACTION );
        return this;
    }
    
    private void unpackResponse( Response<Void> response, GraphDatabaseService graphDb, TxHandler txHandler )
    {
        try
        {
            MasterUtil.applyReceivedTransactions( response, graphDb, txHandler );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to apply received transactions", e );
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
    
    public void close()
    {
        client.shutdown();
    }
}
