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
import java.util.Arrays;
import java.util.TimeZone;

import org.neo4j.helpers.Format;
import org.neo4j.kernel.impl.transaction.command.LogHandler;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersions.CURRENT_LOG_ENTRY_VERSION;

public class LogEntryStart extends AbstractLogEntry
{
    public static final byte[] EMPTY_ADDITIONAL_ARRAY = new byte[]{};

    private final int masterId;
    private final int authorId;
    private final long timeWritten;
    private final long lastCommittedTxWhenTransactionStarted;
    private final byte[] additionalHeader;
    private LogPosition startPosition;

    public LogEntryStart( int masterId, int authorId, long timeWritten,
                          long lastCommittedTxWhenTransactionStarted, byte[] additionalHeader,
                          LogPosition startPosition )
    {
        this( CURRENT_LOG_ENTRY_VERSION, masterId, authorId, timeWritten,
                lastCommittedTxWhenTransactionStarted, additionalHeader, startPosition );
    }

    public LogEntryStart( byte version, int masterId, int authorId, long timeWritten,
                          long lastCommittedTxWhenTransactionStarted, byte[] additionalHeader, LogPosition startPosition )
    {
        super( TX_START, version );
        this.masterId = masterId;
        this.authorId = authorId;
        this.startPosition = startPosition;
        this.timeWritten = timeWritten;
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        this.additionalHeader = additionalHeader;
    }

    public int getMasterId()
    {
        return masterId;
    }

    public int getLocalId()
    {
        return authorId;
    }

    public LogPosition getStartPosition()
    {
        return startPosition;
    }

    public void setStartPosition( LogPosition position )
    {
        this.startPosition = position;
    }

    public long getTimeWritten()
    {
        return timeWritten;
    }

    public long getLastCommittedTxWhenTransactionStarted()
    {
        return lastCommittedTxWhenTransactionStarted;
    }

    public byte[] getAdditionalHeader()
    {
        return additionalHeader;
    }

    /**
     * @return combines necessary state to get a unique checksum to identify this transaction uniquely.
     */
    public static long checksum( byte[] additionalHeader, int masterId, int authorId )
    {
        // [4 bits combined masterId/myId][4 bits xid hashcode, which combines time/randomness]
        long lowBits = Arrays.hashCode( additionalHeader );
        long highBits = masterId*37 + authorId;
        return (highBits << 32) | (lowBits & 0xFFFFFFFFL);
    }

    public static long checksum( LogEntryStart entry )
    {
        return checksum( entry.additionalHeader, entry.masterId, entry.authorId );
    }

    public long checksum()
    {
        return checksum( this );
    }

    @Override
    public String toString()
    {
        return toString( Format.DEFAULT_TIME_ZONE );
    }

    @Override
    public void accept( LogHandler handler ) throws IOException
    {
        handler.startEntry( this );
    }

    @Override
    public String toString( TimeZone timeZone )
    {
        return "Start[" +
                "master=" + masterId + "," +
                "me=" + authorId + "," +
                "time=" + timestamp( timeWritten, timeZone ) + "," +
                "lastCommittedTxWhenTransactionStarted=" + lastCommittedTxWhenTransactionStarted + "," +
                "additionalHeaderLength=" + (additionalHeader == null ? -1 : additionalHeader.length) + "," +
                "position=" + startPosition + "," +
                "checksum=" + checksum( this ) +
                "]";
    }

    @Override
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

        LogEntryStart start = (LogEntryStart) o;

        if ( authorId != start.authorId )
        {
            return false;
        }
        if ( lastCommittedTxWhenTransactionStarted != start.lastCommittedTxWhenTransactionStarted )
        {
            return false;
        }
        if ( masterId != start.masterId )
        {
            return false;
        }
        if ( timeWritten != start.timeWritten )
        {
            return false;
        }
        if ( !Arrays.equals( additionalHeader, start.additionalHeader ) )
        {
            return false;
        }
        if ( !startPosition.equals( start.startPosition ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = masterId;
        result = 31 * result + authorId;
        result = 31 * result + (int) (timeWritten ^ (timeWritten >>> 32));
        result = 31 * result + (int) (lastCommittedTxWhenTransactionStarted ^ (lastCommittedTxWhenTransactionStarted >>> 32));
        result = 31 * result + (additionalHeader != null ? Arrays.hashCode( additionalHeader ) : 0);
        result = 31 * result + startPosition.hashCode();
        return result;
    }
}
