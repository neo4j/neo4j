package org.neo4j.backup;

import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreWriter;

public interface TheBackupInterface
{
    Response<Void> fullBackup( StoreWriter writer );
    
    Response<Void> incrementalBackup( SlaveContext context );
}
