/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;

public class LuceneLogTranslator implements Function<List<LogEntry>, List<LogEntry>>
{
    @Override
    public List<LogEntry> apply( List<LogEntry> logEntries )
    {
        final List<LogEntry> newEntries = new ArrayList<>();
        for ( LogEntry logEntry : logEntries )
        {
            try
            {
                logEntry.accept( new LogHandler.Filter( null )
                {
                    @Override
                    public void startEntry( LogEntry.Start startEntry ) throws IOException
                    {
                        newEntries.add( new LogEntry.Start( startEntry.getMasterId(), startEntry.getLocalId(),
                                startEntry.getTimeWritten(), startEntry.getLastCommittedTxWhenTransactionStarted(),
                                startEntry.getAdditionalHeader(), startEntry.getStartPosition() ) );
                    }

                    @Override
                    public void onePhaseCommitEntry( LogEntry.OnePhaseCommit onePhaseCommitEntry ) throws IOException
                    {
                        newEntries.add( new LogEntry.OnePhaseCommit( LogEntry.CURRENT_LOG_ENTRY_VERSION,
                                onePhaseCommitEntry.getTxId(), onePhaseCommitEntry.getTimeWritten() ) );
                    }

                    @Override
                    public void commandEntry( LogEntry.Command commandEntry ) throws IOException
                    {
                        newEntries.add( new LogEntry.Command( LogEntry.CURRENT_LOG_ENTRY_VERSION, commandEntry
                                .getXaCommand() ) );
                    }
                } );
            }
            catch ( IOException e )
            {
                // Won't happen, but anyway
                throw new RuntimeException( e );
            }
        }
        return newEntries;
    }
}
