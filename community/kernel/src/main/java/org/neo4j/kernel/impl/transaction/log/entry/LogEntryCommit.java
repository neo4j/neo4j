/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.TimeZone;

import org.neo4j.helpers.Format;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.CURRENT;

public class LogEntryCommit extends AbstractLogEntry
{
    private final long txId;
    private final long timeWritten;
    protected final String name;

    public LogEntryCommit( long txId, long timeWritten )
    {
        this( CURRENT, txId, timeWritten );
    }

    public LogEntryCommit( LogEntryVersion version, long txId, long timeWritten )
    {
        super( version, TX_COMMIT );
        this.txId = txId;
        this.timeWritten = timeWritten;
        this.name = "Commit";
    }

    public long getTxId()
    {
        return txId;
    }

    public long getTimeWritten()
    {
        return timeWritten;
    }

    @Override
    public String toString()
    {
        return toString( Format.DEFAULT_TIME_ZONE );
    }

    @Override
    public String toString( TimeZone timeZone )
    {
        return name + "[txId=" + getTxId() + ", " + timestamp( getTimeWritten(), timeZone ) + "]";
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public <T extends LogEntry> T as()
    {
        return (T) this;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        LogEntryCommit commit = (LogEntryCommit) o;
        return timeWritten == commit.timeWritten && txId == commit.txId && name.equals( commit.name );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (txId ^ (txId >>> 32));
        result = 31 * result + (int) (timeWritten ^ (timeWritten >>> 32));
        result = 31 * result + name.hashCode();
        return result;
    }
}
