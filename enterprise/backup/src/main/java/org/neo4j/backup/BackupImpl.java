package org.neo4j.backup;

import org.neo4j.com.MasterUtil;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreWriter;
import org.neo4j.graphdb.GraphDatabaseService;

class BackupImpl implements TheBackupInterface
{
    private final GraphDatabaseService graphDb;

    public BackupImpl( GraphDatabaseService graphDb )
    {
        this.graphDb = graphDb;
    }
    
    public Response<Void> fullBackup( StoreWriter writer )
    {
        SlaveContext context = MasterUtil.rotateLogsAndStreamStoreFiles( graphDb, writer );
        writer.done();
        return MasterUtil.packResponse( graphDb, context, null, MasterUtil.ALL );
    }
    
    public Response<Void> incrementalBackup( SlaveContext context )
    {
        return MasterUtil.packResponse( graphDb, context, null, MasterUtil.ALL );
    }
}
