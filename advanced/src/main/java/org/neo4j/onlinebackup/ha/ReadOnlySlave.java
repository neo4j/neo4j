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
import org.neo4j.onlinebackup.net.SocketException;

public class ReadOnlySlave implements Callback
{
    private final EmbeddedReadOnlyGraphDatabase graphDb;
    private final NeoStoreXaDataSource xaDs;

    private final JobEater jobEater;
    private final LogApplier logApplier;
    
    private final String masterIp;
    private final int masterPort;
    private final Connection masterConnection;
    
    private long masterVersion; 
    
    public ReadOnlySlave( String path, Map<String,String> params, 
        String masterIp, int masterPort )
    {
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
        if ( !masterConnection.connected() )
        {
            if ( masterConnection.connectionRefused() )
            {
                throw new SocketException( "Connection to master[" + masterIp + 
                    ":" + masterPort + "] refused" );
            }
        }
        jobEater.addJob( new ConnectToMasterJob( masterConnection, this ) );
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
    
    public GraphDatabaseService getGraphDbService()
    {
        return graphDb;
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
        if ( job instanceof ConnectToMasterJob )
        {
            ConnectToMasterJob connectJob = (ConnectToMasterJob) job;
            masterVersion = connectJob.getMasterVersion();
            if ( masterVersion > getVersion() )
            {
                // request log versions
            }
        }
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

    public void tryApplyNewLog()
    {
        long nextVersion = xaDs.getCurrentLogVersion();
        while ( xaDs.hasLogicalLog( nextVersion ) )
        {
           logApplier.applyLog( nextVersion );
           nextVersion++;
        }
        // 
    }
}
