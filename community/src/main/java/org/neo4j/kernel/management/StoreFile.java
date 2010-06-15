package org.neo4j.kernel.management;

import java.io.File;

public class StoreFile extends Neo4jJmx implements StoreFileMBean
{
    private static final String NODE_STORE = "neostore.nodestore.db";
    private static final String RELATIONSHIP_STORE = "neostore.relationshipstore.db";
    private static final String PROPERTY_STORE = "neostore.propertystore.db";
    private static final String ARRAY_STORE = "neostore.propertystore.db.arrays";
    private static final String STRING_STORE = "neostore.propertystore.db.strings";
    private static final String LOGICAL_LOG1 = "nioneo_logical.log.1";
    private static final String LOGICAL_LOG2 = "nioneo_logical.log.2";
    private final File storePath;

    StoreFile( int instanceId, File storePath )
    {
        super( instanceId );
        this.storePath = storePath;
    }

    public long getTotalStoreSize()
    {
        return sizeOf( storePath );
    }

    public long getLogicalLogSize()
    {
        File logicalLog = new File( storePath, LOGICAL_LOG1 );
        if ( !logicalLog.isFile() )
        {
            logicalLog = new File( storePath, LOGICAL_LOG2 );
        }
        return sizeOf( logicalLog );
    }

    private static long sizeOf( File file )
    {
        if ( file.isFile() )
        {
            return file.length();
        }
        else if ( file.isDirectory() )
        {
            long size = 0;
            for ( File child : file.listFiles() )
            {
                size += sizeOf( child );
            }
            return size;
        }
        return 0;
    }

    private long sizeOf( String name )
    {
        return sizeOf( new File( storePath, name ) );
    }

    public long getArrayStoreSize()
    {
        return sizeOf( ARRAY_STORE );
    }

    public long getNodeStoreSize()
    {
        return sizeOf( NODE_STORE );
    }

    public long getPropertyStoreSize()
    {
        return sizeOf( PROPERTY_STORE );
    }

    public long getRelationshipStoreSize()
    {
        return sizeOf( RELATIONSHIP_STORE );
    }

    public long getStringStoreSize()
    {
        return sizeOf( STRING_STORE );
    }
}
