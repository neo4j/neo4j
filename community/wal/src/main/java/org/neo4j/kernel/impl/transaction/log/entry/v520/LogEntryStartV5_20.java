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
package org.neo4j.kernel.impl.transaction.log.entry.v520;

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.string.Mask;

public class LogEntryStartV5_20 extends LogEntryStart {
    private final long appendIndex;
    private final int previousChecksum;

    public LogEntryStartV5_20(
            KernelVersion kernelVersion,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            long appendIndex,
            int previousChecksum,
            byte[] additionalHeader,
            LogPosition startPosition) {
        super(kernelVersion, timeWritten, lastCommittedTxWhenTransactionStarted, additionalHeader, startPosition);
        this.previousChecksum = previousChecksum;
        this.appendIndex = appendIndex;
    }

    @Override
    public int getPreviousChecksum() {
        return previousChecksum;
    }

    @Override
    public long getAppendIndex() {
        return appendIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryStartV5_20[" + "kernelVersion=" + kernelVersion() + ",time=" + timestamp(timeWritten)
                + ",lastCommittedTxWhenTransactionStarted=" + lastCommittedTxWhenTransactionStarted
                + ",additionalHeaderLength=" + (additionalHeader == null ? -1 : additionalHeader.length) + ","
                + (additionalHeader == null ? "" : Arrays.toString(additionalHeader))
                + ", appendIndex="
                + appendIndex + ", previousChecksum=" + previousChecksum + ", position=" + startPosition + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogEntryStartV5_20 that = (LogEntryStartV5_20) o;
        return appendIndex == that.appendIndex && previousChecksum == that.previousChecksum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), appendIndex, previousChecksum);
    }
}
