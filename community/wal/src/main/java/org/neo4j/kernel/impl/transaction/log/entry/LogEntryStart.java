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

import java.util.Arrays;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.string.Mask;

public class LogEntryStart extends AbstractVersionAwareLogEntry {
    public static final int MAX_ADDITIONAL_HEADER_SIZE = Long.BYTES;

    protected final long timeWritten;
    protected final long lastCommittedTxWhenTransactionStarted;
    protected final byte[] additionalHeader;
    protected final LogPosition startPosition;

    protected LogEntryStart(
            KernelVersion kernelVersion,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            byte[] additionalHeader,
            LogPosition startPosition) {
        super(kernelVersion, TX_START);
        this.startPosition = startPosition;
        this.timeWritten = timeWritten;
        this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
        this.additionalHeader = additionalHeader;
    }

    public LogPosition getStartPosition() {
        return startPosition;
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

    public int getPreviousChecksum() {
        return 0;
    }

    @Override
    public String toString(Mask mask) {
        return "Start[" + "kernelVersion="
                + kernelVersion() + ",time="
                + timestamp(timeWritten) + ",lastCommittedTxWhenTransactionStarted="
                + lastCommittedTxWhenTransactionStarted + ",additionalHeaderLength="
                + (additionalHeader == null ? -1 : additionalHeader.length) + ","
                + (additionalHeader == null ? "" : Arrays.toString(additionalHeader))
                + "," + "position="
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

        LogEntryStart start = (LogEntryStart) o;

        return lastCommittedTxWhenTransactionStarted == start.lastCommittedTxWhenTransactionStarted
                && timeWritten == start.timeWritten
                && kernelVersion() == start.kernelVersion()
                && Arrays.equals(additionalHeader, start.additionalHeader)
                && startPosition.equals(start.startPosition);
    }

    @Override
    public int hashCode() {
        int result = (int) (timeWritten ^ (timeWritten >>> 32));
        result = 31 * result
                + (int) (lastCommittedTxWhenTransactionStarted ^ (lastCommittedTxWhenTransactionStarted >>> 32));
        result = 31 * result + (additionalHeader != null ? Arrays.hashCode(additionalHeader) : 0);
        result = 31 * result + startPosition.hashCode();
        return result;
    }
}
