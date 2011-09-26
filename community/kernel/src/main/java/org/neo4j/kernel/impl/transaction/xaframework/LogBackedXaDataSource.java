/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.LogExtractor;

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
    public boolean deleteLogicalLog( long version )
    {
        return logicalLog.deleteLogicalLog( version );
    }

    @Override
    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        return logicalLog.getLogicalLogOrMyselfCommitted( version, 0 );
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
    public long rotateLogicalLog() throws IOException
    {
        return logicalLog.rotate();
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
    public String getFileName( long version )
    {
        return logicalLog.getFileName( version );
    }

    @Override
    public ReadableByteChannel getPreparedTransaction( int identifier ) throws IOException
    {
        return logicalLog.getPreparedTransaction( identifier );
    }

    @Override
    public void getPreparedTransaction( int identifier, LogBuffer targetBuffer ) throws IOException
    {
        logicalLog.getPreparedTransaction( identifier, targetBuffer );
    }

    @Override
    public int getMasterForCommittedTx( long txId ) throws IOException
    {
        return logicalLog.getMasterIdForCommittedTransaction( txId );
    }
    
    @Override
    public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
    {
        return logicalLog.getLogExtractor( startTxId, endTxIdHint );
    }

    protected void setKeepLogicalLogsIfSpecified( String configString, String dataSourceName )
    {
        Boolean keepLogs = shouldKeepLog( configString, dataSourceName );
        if ( keepLogs != null )
        {
            getXaContainer().getLogicalLog().setKeepLogs( keepLogs.booleanValue() );
        }
    }
}