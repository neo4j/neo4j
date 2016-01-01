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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;

public class LogFilter implements LogHandler
{
    private final Function<List<LogEntry>, List<LogEntry>> interceptor;
    private final LogHandler delegate;
    private final List<LogEntry> logEntries;

    public LogFilter( Function<List<LogEntry>, List<LogEntry>> interceptor, LogHandler delegate )
    {
        this.interceptor = interceptor;
        this.delegate = delegate;
        logEntries = new ArrayList<>();
    }

    @Override
    public void startLog()
    {
        logEntries.clear();
    }

    @Override
    public void startEntry( LogEntry.Start startEntry )
    {
        logEntries.add( startEntry );
    }

    @Override
    public void prepareEntry( LogEntry.Prepare prepareEntry )
    {
        logEntries.add( prepareEntry );
    }

    @Override
    public void onePhaseCommitEntry( LogEntry.OnePhaseCommit onePhaseCommitEntry )
    {
        logEntries.add( onePhaseCommitEntry );
    }

    @Override
    public void twoPhaseCommitEntry( LogEntry.TwoPhaseCommit twoPhaseCommitEntry )
    {
        logEntries.add( twoPhaseCommitEntry );
    }

    @Override
    public void doneEntry( LogEntry.Done doneEntry )
    {
        logEntries.add( doneEntry );
    }

    @Override
    public void commandEntry( LogEntry.Command commandEntry )
    {
        logEntries.add( commandEntry );
    }

    @Override
    public void endLog( boolean success ) throws IOException
    {
        List<LogEntry> filtered = interceptor.apply( logEntries );

        delegate.startLog();
        for ( LogEntry entry : filtered )
        {
            entry.accept( delegate );
        }
        delegate.endLog( success );

        logEntries.clear();
    }
}
