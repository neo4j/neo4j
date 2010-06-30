package org.neo4j.kernel.impl.management;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.management.Kernel;

class KernelBean extends Neo4jMBean implements Kernel
{
    private final long kernelStartTime;
    private final long storeCreationDate;
    private final long storeId;
    private final long storeLogVersion;
    private final boolean isReadOnly;
    private final String kernelVersion;
    private final String storeDir;

    KernelBean( int instanceId, String kernelVersion, NeoStoreXaDataSource datasource )
            throws NotCompliantMBeanException
    {
        super( instanceId, Kernel.class );
        this.kernelVersion = kernelVersion;
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

    public Date getKernelStartTime()
    {
        return new Date( kernelStartTime );
    }

    public Date getStoreCreationDate()
    {
        return new Date( storeCreationDate );
    }

    public String getStoreId()
    {
        return Long.toHexString( storeId );
    }

    public long getStoreLogVersion()
    {
        return storeLogVersion;
    }

    public String getKernelVersion()
    {
        return kernelVersion;
    }

    public boolean isReadOnly()
    {
        return isReadOnly;
    }

    public String getStoreDirectory()
    {
        return storeDir;
    }
}
