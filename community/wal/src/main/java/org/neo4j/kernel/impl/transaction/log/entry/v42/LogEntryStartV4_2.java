/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.entry.v42;

import java.util.Arrays;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.string.Mask;

public class LogEntryStartV4_2 extends LogEntryStart {
    private final int previousChecksum;

    public LogEntryStartV4_2(
            KernelVersion version,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            int previousChecksum,
            byte[] additionalHeader,
            LogPosition startPosition) {
        super(version, timeWritten, lastCommittedTxWhenTransactionStarted, additionalHeader, startPosition);
        this.previousChecksum = previousChecksum;
    }

    @Override
    public int getPreviousChecksum() {
        return previousChecksum;
    }

    @Override
    public String toString(Mask mask) {
        return "Start[" + "kernelVersion="
                + kernelVersion() + ",time="
                + timestamp(timeWritten) + ",lastCommittedTxWhenTransactionStarted="
                + lastCommittedTxWhenTransactionStarted + ",additionalHeaderLength="
                + (additionalHeader == null ? -1 : additionalHeader.length) + ","
                + (additionalHeader == null ? "" : Arrays.toString(additionalHeader))
                + ",previousChecksum=" + previousChecksum + "," + "position="
                + startPosition + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogEntryStartV4_2 start = (LogEntryStartV4_2) o;

        return lastCommittedTxWhenTransactionStarted == start.lastCommittedTxWhenTransactionStarted
                && timeWritten == start.timeWritten
                && previousChecksum == start.previousChecksum
                && Arrays.equals(additionalHeader, start.additionalHeader)
                && startPosition.equals(start.startPosition);
    }

    @Override
    public int hashCode() {
        int result = (int) (timeWritten ^ (timeWritten >>> 32));
        result = 31 * result
                + (int) (lastCommittedTxWhenTransactionStarted ^ (lastCommittedTxWhenTransactionStarted >>> 32));
        result = 31 * result + previousChecksum;
        result = 31 * result + (additionalHeader != null ? Arrays.hashCode(additionalHeader) : 0);
        result = 31 * result + startPosition.hashCode();
        return result;
    }
}
