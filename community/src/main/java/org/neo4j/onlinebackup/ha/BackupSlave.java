package org.neo4j.onlinebackup.ha;

import java.util.Map;

import org.neo4j.onlinebackup.net.Callback;

public class BackupSlave extends AbstractSlave implements Callback
{
    public BackupSlave( String path, Map<String,String> params, 
        String masterIp, int masterPort )
    {
        super( path, params, masterIp, masterPort );
    }
}