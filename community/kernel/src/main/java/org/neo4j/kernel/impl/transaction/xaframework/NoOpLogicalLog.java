/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.regex.Pattern;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

public class NoOpLogicalLog extends XaLogicalLog
{
    public NoOpLogicalLog( Logging logging )
    {
        super( null, null, null, null, null, new Monitors(), logging, null, null, null, 10000l, null );
    }

    @Override
    synchronized void open() throws IOException
    {
        super.open();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean scanIsComplete()
    {
        return true;
    }

    @Override
    public synchronized int start( Xid xid, int masterId, int myId, long highestKnownCommittedTx )
    {
        return 0;
    }

    @Override
    public synchronized void writeStartEntry( int identifier ) throws XAException
    {

    }

    @Override
    synchronized LogEntry.Start getStartEntry( int identifier )
    {
        return null;
    }

    @Override
    public synchronized void prepare( int identifier ) throws XAException
    {

    }

    @Override
    public synchronized void commitOnePhase( int identifier, long txId, ForceMode forceMode ) throws XAException
    {

    }

    @Override
    public synchronized void done( int identifier ) throws XAException
    {

    }

    @Override
    synchronized void doneInternal( int identifier ) throws IOException
    {

    }

    @Override
    public synchronized void commitTwoPhase( int identifier, long txId, ForceMode forceMode ) throws XAException
    {

    }

    @Override
    public synchronized void writeCommand( XaCommand command, int identifier ) throws IOException
    {

    }

    @Override
    public synchronized void close() throws IOException
    {

    }

    @Override
    void reset()
    {
        super.reset();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    void registerTxIdentifier( int identifier )
    {

    }

    @Override
    void unregisterTxIdentifier()
    {

    }

    @Override
    public int getCurrentTxIdentifier()
    {
        return 0;
    }

    @Override
    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        return null;
    }

    @Override
    public ReadableByteChannel getLogicalLog( long version, long position ) throws IOException
    {
        return null;
    }

    @Override
    public synchronized ReadableByteChannel getPreparedTransaction( int identifier ) throws IOException
    {
        return null;
    }

    @Override
    public synchronized void getPreparedTransaction( int identifier, LogBuffer targetBuffer ) throws IOException
    {

    }

    @Override
    public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
    {
        return null;
    }

    @Override
    public synchronized Pair<Integer, Long> getMasterForCommittedTransaction( long txId ) throws IOException
    {
        return null;
    }

    @Override
    public ReadableByteChannel getLogicalLogOrMyselfCommitted( long version, long position ) throws IOException
    {
        return null;
    }

    @Override
    public long getLogicalLogLength( long version )
    {
        return 0l;
    }

    @Override
    public boolean hasLogicalLog( long version )
    {
        return false;
    }

    @Override
    public boolean deleteLogicalLog( long version )
    {
        return false;
    }

    @Override
    protected LogDeserializer getLogDeserializer( ReadableByteChannel byteChannel )
    {
        return null;
    }

    @Override
    public synchronized void applyTransactionWithoutTxId( ReadableByteChannel byteChannel, long nextTxId,
                                                          ForceMode forceMode ) throws IOException
    {

    }

    @Override
    public synchronized void applyTransaction( ReadableByteChannel byteChannel ) throws IOException
    {

    }

    @Override
    public synchronized long rotate() throws IOException
    {
        return 0l;
    }

    @Override
    public void setAutoRotateLogs( boolean autoRotate )
    {

    }

    @Override
    public boolean isLogsAutoRotated()
    {
        return false;
    }

    @Override
    public void setLogicalLogTargetSize( long size )
    {

    }

    @Override
    public long getLogicalLogTargetSize()
    {
        return 0l;
    }

    @Override
    public File getFileName( long version )
    {
        return null;
    }

    @Override
    public File getBaseFileName()
    {
        return null;
    }

    @Override
    public Pattern getHistoryFileNamePattern()
    {
        return null;
    }

    @Override
    public boolean wasNonClean()
    {
        return false;
    }

    @Override
    public long getHighestLogVersion()
    {
        return 0l;
    }

    @Override
    public Long getFirstCommittedTxId( long version )
    {
        return 0l;
    }

    @Override
    public long getLastCommittedTxId()
    {
        return 0l;
    }

    @Override
    public Long getFirstStartRecordTimestamp( long version ) throws IOException
    {
        return 0l;
    }
}
