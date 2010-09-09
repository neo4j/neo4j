package org.neo4j.kernel.impl.management;

import java.io.File;
import java.io.IOException;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.management.StoreFile;

@Service.Implementation( ManagementBeanProvider.class )
public final class StoreFileBean extends ManagementBeanProvider
{
    public StoreFileBean()
    {
        super( StoreFile.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new StoreFileImpl( this, kernel );
    }

    @Description( "Information about the sizes of the different parts of the Neo4j graph store" )
    private static class StoreFileImpl extends Neo4jMBean implements StoreFile
    {
        private static final String NODE_STORE = "neostore.nodestore.db";
        private static final String RELATIONSHIP_STORE = "neostore.relationshipstore.db";
        private static final String PROPERTY_STORE = "neostore.propertystore.db";
        private static final String ARRAY_STORE = "neostore.propertystore.db.arrays";
        private static final String STRING_STORE = "neostore.propertystore.db.strings";
        private static final String LOGICAL_LOG1 = "nioneo_logical.log.1";
        private static final String LOGICAL_LOG2 = "nioneo_logical.log.2";
        private final File storePath;

        StoreFileImpl( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            NeoStoreXaDataSource nioneodb = KernelBean.getNeoDataSource( kernel );
            File path;
            try
            {
                path = new File( nioneodb.getStoreDir() ).getCanonicalFile().getAbsoluteFile();
            }
            catch ( IOException e )
            {
                path = new File( nioneodb.getStoreDir() ).getAbsoluteFile();
            }
            this.storePath = path;
        }

        @Description( "The total disk space used by this Neo4j instance" )
        public long getTotalStoreSize()
        {
            return sizeOf( storePath );
        }

        @Description( "The amount of disk space used by the current Neo4j logical log" )
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

        @Description( "The amount of disk space used to store array properties" )
        public long getArrayStoreSize()
        {
            return sizeOf( ARRAY_STORE );
        }

        @Description( "The amount of disk space used to store nodes" )
        public long getNodeStoreSize()
        {
            return sizeOf( NODE_STORE );
        }

        @Description( "The amount of disk space used to store properties "
                      + "(excluding string values and array values)" )
        public long getPropertyStoreSize()
        {
            return sizeOf( PROPERTY_STORE );
        }

        @Description( "The amount of disk space used to store relationships" )
        public long getRelationshipStoreSize()
        {
            return sizeOf( RELATIONSHIP_STORE );
        }

        @Description( "The amount of disk space used to store string properties" )
        public long getStringStoreSize()
        {
            return sizeOf( STRING_STORE );
        }
    }
}
