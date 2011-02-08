package org.neo4j.backup;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;

@Service.Implementation( KernelExtension.class )
public class OnlineBackupExtension extends KernelExtension
{
    public OnlineBackupExtension()
    {
        super( "online backup" );
    }
    
    @Override
    protected void load( KernelData kernel )
    {
        String configValue = (String) kernel.getConfig().getParams().get( "enable_online_backup" );
        configValue = configValue == null ? "true" : configValue;
        boolean enabled = Boolean.parseBoolean( configValue );
        if ( enabled )
        {
            TheBackupInterface backup = new BackupImpl( kernel.graphDatabase() );
            BackupServer server = new BackupServer( backup, BackupServer.DEFAULT_PORT, null );
            kernel.setState( this, server );
        }
    }
    
    @Override
    protected void unload( KernelData kernel )
    {
        BackupServer server = (BackupServer) kernel.getState( this );
        if ( server != null )
        {
            server.shutdown();
        }
    }
}
