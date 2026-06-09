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
package org.neo4j.kernel.impl.transaction.log.entry.vGloriousFuture;

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.v202505.LogEntryStartV2025_05;
import org.neo4j.string.Mask;

public class LogEntryStartVGloriousFuture extends LogEntryStartV2025_05 {
    private final long transactionSequenceNumber;

    public LogEntryStartVGloriousFuture(
            KernelVersion kernelVersion,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            long appendIndex,
            long transactionSequenceNumber,
            byte[] additionalHeader) {
        super(kernelVersion, timeWritten, lastCommittedTxWhenTransactionStarted, appendIndex, additionalHeader);
        this.transactionSequenceNumber = transactionSequenceNumber;
    }

    @Override
    public long getTransactionSequenceNumber() {
        return transactionSequenceNumber;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryStartVGloriousFuture[" + "kernelVersion=" + kernelVersion() + ",time=" + timestamp(timeWritten)
                + ",lastCommittedTxWhenTransactionStarted=" + lastCommittedTxWhenTransactionStarted
                + ",additionalHeaderLength=" + (additionalHeader == null ? -1 : additionalHeader.length) + ","
                + (additionalHeader == null ? "" : Arrays.toString(additionalHeader))
                + ", appendIndex=" + appendIndex
                + ", transactionSequenceNumber=" + transactionSequenceNumber
                + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        LogEntryStartVGloriousFuture start = (LogEntryStartVGloriousFuture) o;
        return transactionSequenceNumber == start.transactionSequenceNumber;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return Objects.hash(result, transactionSequenceNumber);
    }
}
