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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.string.Mask;

public abstract class LogEntryStart extends AbstractVersionAwareLogEntry {
    public static final int MAX_ADDITIONAL_HEADER_SIZE = Long.BYTES;

    protected final long timeWritten;
    protected final long lastCommittedTxWhenTransactionStarted;
    protected final byte[] additionalHeader;

    protected LogEntryStart(
            KernelVersion kernelVersion,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            byte[] additionalHeader) {
        super(kernelVersion, TX_START);
        this.timeWritten = timeWritten;
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        this.additionalHeader = additionalHeader;
    }

    public long getTimeWritten() {
        return timeWritten;
    }

    public long getLastCommittedTxWhenTransactionStarted() {
        return lastCommittedTxWhenTransactionStarted;
    }

    public byte[] getAdditionalHeader() {
        return additionalHeader;
    }

    public long getAppendIndex() {
        return BASE_APPEND_INDEX;
    }

    public long getTransactionSequenceNumber() {
        return UNKNOWN_TX_SEQUENCE_NUMBER;
    }

    @Override
    public abstract String toString(Mask mask);

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();
}
