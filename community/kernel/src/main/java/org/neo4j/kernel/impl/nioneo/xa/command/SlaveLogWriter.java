/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa.command;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriter;

public class SlaveLogWriter extends LogHandler.Filter
{
    private LogBuffer writeBuffer;

    private final LogWriter.SPI spi;
    private final LogEntryWriter logEntryWriter;

    public SlaveLogWriter( LogHandler applier, LogWriter.SPI spi, LogEntryWriter logEntryWriter )
    {
        super( applier );
        this.logEntryWriter = logEntryWriter;
        this.spi = spi;
    }

    @Override
    public void startLog()
    {
        writeBuffer = spi.getWriteBuffer();
        super.startLog();
    }

    @Override
    public void startEntry( LogEntry.Start startEntry ) throws IOException
    {
        /*
         * You are wondering what is going on here. Let me take you on a journey
         * A transaction, call it A starts, prepares locally, goes to the master and commits there
         *  but doesn't quite make it back here, meaning its application is pending, with only the
         *  start, command  and possibly prepare entries but not the commit, the Xid in xidmap
         * Another transaction, B, does an operation that requires going to master and pull updates - does
         *  that, gets all transactions not present locally (hence, A as well) and injects it.
         *  The Start entry is the first one extracted - if we try to apply it it will throw a Start
         *  entry already injected exception, since the Xid will match an ongoing transaction. If we
         *  had written that to the log recovery would be impossible, constantly throwing the same
         *  exception. So first apply, then write to log.
         * However we cannot do that for every entry - commit must always be written to log first, then
         *  applied because a crash in the mean time could cause partially applied transactions.
         *  The start entry does not have this problem because if it fails nothing will ever be applied -
         *  the same goes for commands but we don't care about those.
         */
        startEntry.setStartPosition( writeBuffer.getFileChannelPosition() );
        super.startEntry( startEntry );
        logEntryWriter.writeLogEntry( startEntry, writeBuffer );
    }

    @Override
    public void prepareEntry( LogEntry.Prepare prepareEntry ) throws IOException
    {
        logEntryWriter.writeLogEntry( prepareEntry, writeBuffer );
        super.prepareEntry( prepareEntry );
    }

    @Override
    public void onePhaseCommitEntry( LogEntry.OnePhaseCommit onePhaseCommitEntry ) throws IOException
    {
        logEntryWriter.writeLogEntry( onePhaseCommitEntry, writeBuffer );
        writeBuffer.writeOut();
        super.onePhaseCommitEntry( onePhaseCommitEntry );
    }

    @Override
    public void twoPhaseCommitEntry( LogEntry.TwoPhaseCommit twoPhaseCommitEntry ) throws IOException
    {
        logEntryWriter.writeLogEntry( twoPhaseCommitEntry, writeBuffer );
        writeBuffer.writeOut();
        super.twoPhaseCommitEntry( twoPhaseCommitEntry );
    }

    @Override
    public void doneEntry( LogEntry.Done doneEntry ) throws IOException
    {
        logEntryWriter.writeLogEntry( doneEntry, writeBuffer );
        super.doneEntry( doneEntry );
    }

    @Override
    public void commandEntry( LogEntry.Command commandEntry ) throws IOException
    {
        logEntryWriter.writeLogEntry( commandEntry, writeBuffer );
        super.commandEntry( commandEntry );
    }
}
