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

import java.util.Objects;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.Mask;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;

public class LogEntryCommit extends AbstractLogEntry
{
    private final long txId;
    private final long timeWritten;
    private final int checksum;

    public LogEntryCommit( long txId, long timeWritten, int checksum )
    {
        this( KernelVersion.LATEST, txId, timeWritten, checksum );
    }

    public LogEntryCommit( KernelVersion version, long txId, long timeWritten, int checksum )
    {
        super( version, TX_COMMIT );
        this.txId = txId;
        this.timeWritten = timeWritten;
        this.checksum = checksum;
    }

    public long getTxId()
    {
        return txId;
    }

    public long getTimeWritten()
    {
        return timeWritten;
    }

    public int getChecksum()
    {
        return checksum;
    }

    @Override
    public String toString( Mask mask )
    {
        return "Commit[txId=" + getTxId() + ", " + timestamp( getTimeWritten() ) + ", checksum=" + checksum + "]";
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
        LogEntryCommit that = (LogEntryCommit) o;
        return txId == that.txId && timeWritten == that.timeWritten && checksum == that.checksum;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( txId, timeWritten, checksum );
    }
}
