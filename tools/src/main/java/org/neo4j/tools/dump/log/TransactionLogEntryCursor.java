/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tools.dump.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_COMMIT;

/**
 * Groups {@link LogEntry} instances transaction by transaction
 */
public class TransactionLogEntryCursor implements IOCursor<LogEntry[]>
{
    private final IOCursor<LogEntry> delegate;
    private final List<LogEntry> transaction = new ArrayList<>();

    public TransactionLogEntryCursor( IOCursor<LogEntry> delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public LogEntry[] get()
    {
        return transaction.toArray( new LogEntry[transaction.size()] );
    }

    @Override
    public boolean next() throws IOException
    {
        transaction.clear();
        LogEntry entry;
        while ( delegate.next() )
        {
            entry = delegate.get();
            transaction.add( entry );
            if ( isBreakPoint( entry ) )
            {
                return true;
            }
        }
        return !transaction.isEmpty();
    }

    private static boolean isBreakPoint( LogEntry entry )
    {
        byte type = entry.getType();
        return type == TX_COMMIT || type == CHECK_POINT;
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }
}
