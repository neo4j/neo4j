package org.neo4j.kernel.impl.management;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.management.Kernel;

@Description( "Information about the Neo4j kernel" )
class KernelBean extends Neo4jMBean implements Kernel
{
    private final long kernelStartTime;
    private final long storeCreationDate;
    private final long storeId;
    private final long storeLogVersion;
    private final boolean isReadOnly;
    private final String kernelVersion;
    private final String storeDir;
    private final ObjectName query;

    KernelBean( KernelData kernel )
            throws NotCompliantMBeanException
    {
        super( Kernel.class, kernel );
        NeoStoreXaDataSource datasource = getNeoDataSource( kernel );
        this.kernelVersion = kernel.version();
        this.query = JmxExtension.getObjectName( kernel, null, null );
        storeCreationDate = datasource.getCreationTime();
        storeLogVersion = datasource.getCurrentLogVersion();
        isReadOnly = datasource.isReadOnly();
        storeId = datasource.getRandomIdentifier();

        @SuppressWarnings( "hiding" ) String storeDir;
        try
        {
            storeDir = new File( datasource.getStoreDir() ).getCanonicalFile().getAbsolutePath();
        }
        catch ( IOException e )
        {
            storeDir = new File( datasource.getStoreDir() ).getAbsolutePath();
        }
        this.storeDir = storeDir;

        kernelStartTime = new Date().getTime();
    }

    static NeoStoreXaDataSource getNeoDataSource( KernelData kernel )
    {
        XaDataSourceManager mgr = kernel.getConfig().getTxModule().getXaDataSourceManager();
        return (NeoStoreXaDataSource) mgr.getXaDataSource( "nioneodb" );
    }

    @Description( "An ObjectName that can be used as a query for getting all management "
                  + "beans for this Neo4j instance." )
    public ObjectName getMBeanQuery()
    {
        return query;
    }

    @Description( "The time from which this Neo4j instance was in operational mode" )
    public Date getKernelStartTime()
    {
        return new Date( kernelStartTime );
    }

    @Description( "The time when this Neo4j graph store was created" )
    public Date getStoreCreationDate()
    {
        return new Date( storeCreationDate );
    }

    @Description( "A identifier that uniquely identifies this Neo4j graph store" )
    public String getStoreId()
    {
        return Long.toHexString( storeId );
    }

    @Description( "The current version of the Neo4j store logical log" )
    public long getStoreLogVersion()
    {
        return storeLogVersion;
    }

    @Description( "The version of Neo4j" )
    public String getKernelVersion()
    {
        return kernelVersion;
    }

    @Description( "Whether this is a read only instance" )
    public boolean isReadOnly()
    {
        return isReadOnly;
    }

    @Description( "The location where the Neo4j store is located" )
    public String getStoreDirectory()
    {
        return storeDir;
    }
}
