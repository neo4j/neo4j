/*
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.command.LogHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

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
    public void startEntry( LogEntryStart startEntry ) throws IOException
    {
        entriesFound++;
        super.startEntry( startEntry );
    }

    @Override
    public void onePhaseCommitEntry( OnePhaseCommit onePhaseCommitEntry ) throws IOException
    {
        entriesFound++;
        super.onePhaseCommitEntry( onePhaseCommitEntry );
    }

    @Override
    public void commandEntry( LogEntryCommand commandEntry ) throws IOException
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
