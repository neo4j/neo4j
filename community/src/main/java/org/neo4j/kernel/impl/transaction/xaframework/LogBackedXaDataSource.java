package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

public abstract class LogBackedXaDataSource extends XaDataSource
{
    private XaLogicalLog logicalLog;
    
    public LogBackedXaDataSource( Map<?, ?> params )
            throws InstantiationException
    {
        super( params );
    }
    
    /**
     * Sets the {@link XaLogicalLog} at creation time (in constructor). It is
     * done with this method because it can be so problematic in so many ways
     * to have a subclass pass in this to the constructor.
     * @param logicalLog the {@link XaLogicalLog} to set.
     */
    protected void setLogicalLogAtCreationTime( XaLogicalLog logicalLog )
    {
        if ( this.logicalLog != null )
        {
            throw new RuntimeException( "Logical log already set for " + this );
        }
        this.logicalLog = logicalLog;
    }

    @Override
    public void applyLog( ReadableByteChannel byteChannel ) throws IOException
    {
        logicalLog.applyLog( byteChannel );
    }

    @Override
    public boolean deleteLogicalLog( long version )
    {
        return logicalLog.deleteLogicalLog( version );
    }

    @Override
    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        return logicalLog.getLogicalLog( version );
    }

    @Override
    public long getLogicalLogLength( long version )
    {
        return logicalLog.getLogicalLogLength( version );
    }

    @Override
    public boolean hasLogicalLog( long version )
    {
        return logicalLog.hasLogicalLog( version );
    }

    @Override
    public boolean isLogicalLogKept()
    {
        return logicalLog.isLogsKept();
    }

    @Override
    public void keepLogicalLogs( boolean keepLogs )
    {
        logicalLog.setKeepLogs( keepLogs );
    }

    @Override
    public void rotateLogicalLog() throws IOException
    {
        logicalLog.rotate();
    }

    @Override
    public void setAutoRotate( boolean rotate )
    {
        logicalLog.setAutoRotateLogs( rotate );
    }

    @Override
    public void setLogicalLogTargetSize( long size )
    {
        logicalLog.setLogicalLogTargetSize( size );
    }

    @Override
    public void makeBackupSlave()
    {
        logicalLog.makeBackupSlave();
    }
    
    @Override
    public String getFileName( long version )
    {
        return logicalLog.getFileName( version );
    }
    
    @Override
    public ReadableByteChannel getCommittedTransaction( long txId ) throws IOException
    {
        return logicalLog.getCommittedTransaction( txId );
    }

    @Override
    public ReadableByteChannel getPreparedTransaction( long identifier ) throws IOException
    {
        return logicalLog.getPreparedTransaction( identifier );
    }

    @Override
    public int getMasterForCommittedTx( long txId ) throws IOException
    {
        return logicalLog.getMasterIdForCommittedTransaction( txId );
    }
}