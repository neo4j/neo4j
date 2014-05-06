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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;
import org.neo4j.kernel.impl.util.Consumer;

class LogEntryConsumer implements Consumer<LogEntry, IOException>
{
    private LogEntry.Start startEntry;

    private LogHandler handler;
    private int xidIdentifier;
    private List<LogEntry> entries;
    private final Function<List<LogEntry>, List<LogEntry>> translator;

    LogEntryConsumer( Function<List<LogEntry>, List<LogEntry>> translator )
    {
        this.translator = translator;
    }

    @Override
    public boolean accept( LogEntry logEntry ) throws IOException
    {
        if ( startEntry == null )
        {
            if ( logEntry == null || logEntry.getType() != LogEntry.TX_START )
            {
                throw new IOException( "Unable to find start entry" );
            }
            else
            {
                startEntry = (LogEntry.Start) logEntry;
            }
        }

        logEntry.setIdentifier( xidIdentifier );

        if ( logEntry.getVersion() != LogEntry.CURRENT_LOG_ENTRY_VERSION )
        {
            if ( entries == null )
            {
                entries = new LinkedList<>();
            }
            entries.add( logEntry );

            if ( (logEntry.getType() == LogEntry.TX_1P_COMMIT || logEntry.getType() == LogEntry.TX_2P_COMMIT)  )
            {
                entries = translator.apply( entries );
            }
            if ( logEntry.getType() == LogEntry.DONE )
            {
                for ( LogEntry entry : entries )
                {
                    entry.accept( handler );
                }
                entries = null;
            }
        }
        else
        {
            logEntry.accept( handler );
        }

        return true;
    }

    public void bind( int xidIdentifier, LogHandler handler )
    {
        this.xidIdentifier = xidIdentifier;
        this.handler = handler;
    }
}
