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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;

public class EntryCountingLogHandler extends LogHandler.Filter
{
    private int entriesFound = 0;

    public EntryCountingLogHandler( LogHandler noOp )
    {
        super( noOp );
    }

    @Override
    public void startLog()
    {
        super.startLog();
    }

    @Override
    public void startEntry( LogEntry.Start startEntry ) throws IOException
    {
        entriesFound++;
        super.startEntry( startEntry );
    }

    @Override
    public void prepareEntry( LogEntry.Prepare prepareEntry ) throws IOException
    {
        entriesFound++;
        super.prepareEntry( prepareEntry );
    }

    @Override
    public void onePhaseCommitEntry( LogEntry.OnePhaseCommit onePhaseCommitEntry ) throws IOException
    {
        entriesFound++;
        super.onePhaseCommitEntry( onePhaseCommitEntry );
    }

    @Override
    public void twoPhaseCommitEntry( LogEntry.TwoPhaseCommit twoPhaseCommitEntry ) throws IOException
    {
        entriesFound++;
        super.twoPhaseCommitEntry( twoPhaseCommitEntry );
    }

    @Override
    public void doneEntry( LogEntry.Done doneEntry ) throws IOException
    {
        entriesFound++;
        super.doneEntry( doneEntry );
    }

    @Override
    public void commandEntry( LogEntry.Command commandEntry ) throws IOException
    {
        entriesFound++;
        super.commandEntry( commandEntry );
    }

    @Override
    public void endLog( boolean success ) throws IOException
    {
        super.endLog( success );
    }

    public int getEntriesFound()
    {
        return entriesFound;
    }
}
