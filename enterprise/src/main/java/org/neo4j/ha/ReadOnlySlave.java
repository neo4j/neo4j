package org.neo4j.ha;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.neo4j.api.core.EmbeddedReadOnlyNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;

public class ReadOnlySlave implements Callback
{
    private final EmbeddedReadOnlyNeo neo;
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
        this.neo = new EmbeddedReadOnlyNeo( path, params );
        this.xaDs = (NeoStoreXaDataSource) neo.getConfig().getTxModule()
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
    
    public NeoService getNeoService()
    {
        return neo;
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

    public FileChannel setupLogForWrite( long version ) throws IOException
    {
        return xaDs.createLogForWrite( version );
    }

    public void shutdown()
    {
        jobEater.stopEating();
        logApplier.stopApplyLogs();
        neo.shutdown();
    }

    void tryApplyNewLog()
    {
        long nextVersion = xaDs.getCurrentLogVersion();
        if ( xaDs.hasLogicalLog( nextVersion ) )
        {
           logApplier.applyLog( nextVersion );
        }
        else
        {
            // TODO: request version
        }
    }
}
