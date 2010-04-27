package org.neo4j.kernel.manage;

import java.util.Date;

import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

class Kernel extends Neo4jJmx implements KernelMBean
{
    private final long kernelStartTime;
    private final long storeCreationDate;
    private final long storeId;
    private final long storeLogVersion;
    private final boolean isReadOnly;
    private final String kernelVersion;
    private final String storeDir;

    Kernel( int instanceId, String kernelVersion, NeoStoreXaDataSource datasource )
    {
        super( instanceId );
        this.kernelVersion = kernelVersion;
        storeCreationDate = datasource.getCreationTime();
        storeLogVersion = datasource.getCurrentLogVersion();
        isReadOnly = datasource.isReadOnly();
        storeDir = datasource.getStoreDir();
        storeId = datasource.getRandomIdentifier();
        kernelStartTime = new Date().getTime();
    }

    @Override
    public Date getKernelStartTime()
    {
        return new Date( kernelStartTime );
    }

    @Override
    public Date getStoreCreationDate()
    {
        return new Date( storeCreationDate );
    }

    @Override
    public long getStoreId()
    {
        return storeId;
    }

    @Override
    public long getStoreLogVersion()
    {
        return storeLogVersion;
    }

    @Override
    public String getKernelVersion()
    {
        return kernelVersion;
    }

    @Override
    public boolean isReadOnly()
    {
        return isReadOnly;
    }

    @Override
    public String getStoreDirectory()
    {
        return storeDir;
    }
}
