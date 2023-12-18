/*
 * Copyright (c) "Neo4j"
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

import java.util.Arrays;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.Mask;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

public class LogEntryStart extends AbstractLogEntry
{
    private final long timeWritten;
    private final long lastCommittedTxWhenTransactionStarted;
    private final int previousChecksum;
    private final byte[] additionalHeader;
    private final LogPosition startPosition;

    public LogEntryStart( long timeWritten, long lastCommittedTxWhenTransactionStarted,
            int previousChecksum, byte[] additionalHeader, LogPosition startPosition )
    {
        this( KernelVersion.LATEST, timeWritten, lastCommittedTxWhenTransactionStarted, previousChecksum,
                additionalHeader, startPosition );
    }

    public LogEntryStart( KernelVersion version, long timeWritten, long lastCommittedTxWhenTransactionStarted,
            int previousChecksum, byte[] additionalHeader, LogPosition startPosition )
    {
        super( version, TX_START );
        this.previousChecksum = previousChecksum;
        this.startPosition = startPosition;
        this.timeWritten = timeWritten;
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        this.additionalHeader = additionalHeader;
    }

    public LogPosition getStartPosition()
    {
        return startPosition;
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

    @Override
    public String toString( Mask mask )
    {
        return "Start[" +
                "kernelVersion=" + getVersion() + "," +
                "time=" + timestamp( timeWritten ) + "," +
                "lastCommittedTxWhenTransactionStarted=" + lastCommittedTxWhenTransactionStarted + "," +
                "previousChecksum=" + previousChecksum + "," +
                "additionalHeaderLength=" + (additionalHeader == null ? -1 : additionalHeader.length) + "," +
                (additionalHeader == null ? "" : Arrays.toString( additionalHeader ) ) + "," +
                "position=" + startPosition +
                "]";
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

        return lastCommittedTxWhenTransactionStarted == start.lastCommittedTxWhenTransactionStarted &&
               timeWritten == start.timeWritten &&
               previousChecksum == start.previousChecksum &&
               Arrays.equals( additionalHeader, start.additionalHeader ) && startPosition.equals( start.startPosition );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (timeWritten ^ (timeWritten >>> 32));
        result = 31 * result + (int) (lastCommittedTxWhenTransactionStarted ^ (lastCommittedTxWhenTransactionStarted >>> 32));
        result = 31 * result + previousChecksum;
        result = 31 * result + (additionalHeader != null ? Arrays.hashCode( additionalHeader ) : 0);
        result = 31 * result + startPosition.hashCode();
        return result;
    }

    public int getPreviousChecksum()
    {
        return previousChecksum;
    }
}
