package org.neo4j.onlinebackup;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 * Class that wraps a XA data source.
 */
public abstract class AbstractResource
{

    final XaDataSource xaDs;

    AbstractResource( XaDataSource xaDataSource )
    {
        this.xaDs = xaDataSource;
    }

    abstract public void close();

    public void applyLog( ReadableByteChannel log ) throws IOException
    {
        xaDs.applyLog( log );
    }

    public long getCreationTime()
    {
        return xaDs.getCreationTime();
    }

    public long getIdentifier()
    {
        return xaDs.getCreationTime();
    }

    public String getName()
    {
        return xaDs.getName();
    }

    public boolean hasLogicalLog( long version )
    {
        return xaDs.hasLogicalLog( version );
    }

    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        return xaDs.getLogicalLog( version );
    }

    public long getVersion()
    {
        return xaDs.getCurrentLogVersion();
    }

    public void rotateLog() throws IOException
    {
        xaDs.rotateLogicalLog();
    }

    public void makeBackupSlave()
    {
        xaDs.makeBackupSlave();
    }
}