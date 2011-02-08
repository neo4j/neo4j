package org.neo4j.backup;

import org.jboss.netty.channel.Channel;
import org.neo4j.backup.BackupClient.BackupRequestType;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.com.SlaveContext;

class BackupServer extends Server<TheBackupInterface, Object>
{
    private final BackupRequestType[] contexts = BackupRequestType.values();
    static int DEFAULT_PORT = 6362;
    
    public BackupServer( TheBackupInterface realMaster, int port, String storeDir )
    {
        super( realMaster, port, storeDir );
    }

    @Override
    protected void responseWritten( RequestType<TheBackupInterface> type, Channel channel,
            SlaveContext context )
    {
    }

    @Override
    protected RequestType<TheBackupInterface> getRequestContext( byte id )
    {
        return contexts[id];
    }

    @Override
    protected void finishOffConnection( Channel channel, SlaveContext context )
    {
    }
}
