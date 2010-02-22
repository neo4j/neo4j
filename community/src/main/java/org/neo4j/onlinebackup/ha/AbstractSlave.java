package org.neo4j.onlinebackup.ha;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.onlinebackup.net.Callback;
import org.neo4j.onlinebackup.net.ConnectToMasterJob;
import org.neo4j.onlinebackup.net.Connection;
import org.neo4j.onlinebackup.net.Job;
import org.neo4j.onlinebackup.net.JobEater;

public abstract class AbstractSlave implements Callback
{
    private final EmbeddedReadOnlyGraphDatabase graphDb;
    private final NeoStoreXaDataSource xaDs;

    private final JobEater jobEater;
    private final LogApplier logApplier;
    
    private final String masterIp;
    private final int masterPort;
    private Connection masterConnection;
    
    private long masterVersion; 
    
    public AbstractSlave( String path, Map<String,String> params, 
        String masterIp, int masterPort )
    {
        params.put( "backup_slave", "true" );
        this.graphDb = new EmbeddedReadOnlyGraphDatabase( path, params );
        this.xaDs = (NeoStoreXaDataSource) graphDb.getConfig().getTxModule()
            .getXaDataSourceManager().getXaDataSource( "nioneodb" );
        this.xaDs.makeBackupSlave();
        recover();

        jobEater = new JobEater();
        logApplier = new LogApplier( xaDs );
        jobEater.start();
        logApplier.start();
        
        this.masterIp = masterIp;
        this.masterPort = masterPort;
        masterConnection = new Connection( masterIp, masterPort );
        while ( !masterConnection.connected() )
        {
            if ( masterConnection.connectionRefused() )
            {
                System.out.println( "Unable to connect to master" );
                break;
            }
        }
        if ( masterConnection.connected() )
        {
            jobEater.addJob( new ConnectToMasterJob( masterConnection, this ) );
        }
        System.out.println( "At version: " + getVersion() );
    }
    
    private void recover()
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
    
    public boolean isConnectedToMaster()
    {
        return masterConnection.connected();
    }
    
    public boolean reconnectToMaster()
    {
        masterConnection = new Connection( masterIp, masterPort );
        while ( !masterConnection.connected() )
        {
            if ( masterConnection.connectionRefused() )
            {
                return false;
            }
        }
        jobEater.addJob( new ConnectToMasterJob( masterConnection, this ) );
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
    
    public long getIdentifier()
    {
        return xaDs.getRandomIdentifier();
    }
    
    public long getCreationTime()
    {
        return xaDs.getCreationTime();
    }
    
    public long getVersion()
    {
        return xaDs.getCurrentLogVersion();
    }

    public boolean hasLog( long version )
    {
        return xaDs.hasLogicalLog( version );
    }

    public String getLogName( long version )
    {
        return xaDs.getFileName( version );
    }

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
