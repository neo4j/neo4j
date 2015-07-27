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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.command.LogHandler;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_1P_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;

/**
 * Handles on-the-fly translation of incoming log entries, forwards them to an underlying handler, generally for
 * applying the entries to the store.
 */
class TranslatingEntryVisitor implements Visitor<LogEntry, IOException>
{
    private LogEntryStart startEntry;

    private LogHandler handler;
    private List<LogEntry> entries;
    private final Function<List<LogEntry>, List<LogEntry>> translator;

    TranslatingEntryVisitor( Function<List<LogEntry>, List<LogEntry>> translator )
    {
        this.translator = translator;
    }

    @Override
    public boolean visit( LogEntry logEntry ) throws IOException
    {
        if ( startEntry == null )
        {
            if ( logEntry == null || logEntry.getType() != TX_START )
            {
                throw new IOException( "Unable to find start entry" );
            }
            startEntry = (LogEntryStart) logEntry;
        }

        if ( logEntry.getVersion() != LogEntryVersion.CURRENT )
        {
            if ( entries == null )
            {
                entries = new LinkedList<>();
            }
            entries.add( logEntry );

            if ( logEntry.getType() == TX_1P_COMMIT  )
            {
                entries = translator.apply( entries );
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

    /**
     * Bind this consumer to a transaction handling context as identified by the xidIdentifier and a LogHandler.
     * This is a necessary call before processing a transaction - bad things will happen if two transactions
     * are processed with this consumer without a proper bind() call in between.
     */
    public TranslatingEntryVisitor bind( LogHandler handler )
    {
        this.handler = handler;
        this.entries = null;
        return this;
    }
}
