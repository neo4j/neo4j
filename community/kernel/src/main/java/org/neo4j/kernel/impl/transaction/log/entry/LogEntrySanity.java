/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static java.lang.Math.abs;
import static java.lang.System.currentTimeMillis;

/**
 * Sanity checking for read {@link LogEntry log entries}.
 */
class LogEntrySanity
{
    private static final long UNREASONABLY_LONG_TIME = TimeUnit.DAYS.toMillis( 30 * 365 /*years*/ );
    private static final int UNREASONABLY_HIGH_SERVER_ID = 10_000_000;

    private LogEntrySanity()
    {
        throw new AssertionError();
    }

    static boolean logEntryMakesSense( LogEntry entry )
    {
        if ( entry == null )
        {
            return false;
        }
        if ( entry instanceof LogEntryStart )
        {
            return startEntryMakesSense( (LogEntryStart) entry );
        }
        else if ( entry instanceof LogEntryCommit )
        {
            return commitEntryMakesSense( (LogEntryCommit) entry );
        }
        return true;
    }

    private static boolean commitEntryMakesSense( LogEntryCommit entry )
    {
        return timeMakesSense( entry.getTimeWritten() ) && transactionIdMakesSense( entry );
    }

    private static boolean transactionIdMakesSense( LogEntryCommit entry )
    {
        return entry.getTxId() > TransactionIdStore.BASE_TX_ID;
    }

    private static boolean startEntryMakesSense( LogEntryStart entry )
    {
        return serverIdMakesSense( entry.getLocalId() ) &&
                serverIdMakesSense( entry.getMasterId() ) &&
                timeMakesSense( entry.getTimeWritten() );
    }

    private static boolean serverIdMakesSense( int serverId )
    {
        return serverId >= 0 && serverId < UNREASONABLY_HIGH_SERVER_ID;
    }

    private static boolean timeMakesSense( long time )
    {
        return abs( currentTimeMillis() - time ) < UNREASONABLY_LONG_TIME;
    }
}
