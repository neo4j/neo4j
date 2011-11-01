/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.transaction.xaframework.LogApplier;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogDeserializer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Commit;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;

/**
 * This class implements a transaction stream deserializer which verifies
 * the integrity of the store for each applied transaction. It is assumed that
 * the stream contains a defragmented serialized transaction.
 */
public class VerifyingLogDeserializer implements LogDeserializer
{
    private final ReadableByteChannel byteChannel;
    private final LogBuffer writeBuffer;
    private final LogApplier applier;
    private final XaCommandFactory cf;
    private final ByteBuffer scratchBuffer = ByteBuffer.allocateDirect( 9
                                                                       + Xid.MAXGTRIDSIZE
                                                                       + Xid.MAXBQUALSIZE
                                                                       * 10 );

    private final List<LogEntry> logEntries;

    private LogEntry.Commit commitEntry;
    private LogEntry.Start startEntry;

    VerifyingLogDeserializer( ReadableByteChannel byteChannel,
            LogBuffer writeBuffer, LogApplier applier, XaCommandFactory cf )
    {
        this.byteChannel = byteChannel;
        this.writeBuffer = writeBuffer;
        this.applier = applier;
        this.cf = cf;
        logEntries = new LinkedList<LogEntry>();
    }

    @Override
    public boolean readAndWriteAndApplyEntry( int newXidIdentifier )
            throws IOException
    {
        LogEntry entry = LogIoUtils.readEntry( scratchBuffer, byteChannel, cf );
        if ( entry == null )
        {
            /*
             * The following two cannot run on commit entry found - it is not
             * always present. Even if the stream is incomplete though,
             * XaLogicalLog does take that into account, so we can go ahead and apply.
             */
            verify();
            applyAll();
            return false;
        }
        entry.setIdentifier( newXidIdentifier );
        logEntries.add( entry );
        if ( entry instanceof LogEntry.Commit )
        {
            assert startEntry != null;
            commitEntry = (LogEntry.Commit) entry;
        }
        else if ( entry instanceof LogEntry.Start )
        {
            startEntry = (LogEntry.Start) entry;
        }
        return true;
    }

    @Override
    public Commit getCommitEntry()
    {
        return commitEntry;
    }

    @Override
    public Start getStartEntry()
    {
        return startEntry;
    }

    private void verify()
    {
        /*
         *  Here goes the actual verification code. If it passes,
         *  just return - if not, throw RuntimeExeption so that the
         *  store remains safe.
         */
    }

    private void applyAll() throws IOException
    {
        for ( LogEntry entry : logEntries )
        {
            LogIoUtils.writeLogEntry( entry, writeBuffer );
            applier.apply( entry );
        }
    }
}
