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

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;

import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.string.Mask;

public class LogEntryCommit extends AbstractVersionAwareLogEntry {
    protected final long txId;
    protected final long timeWritten;

    protected LogEntryCommit(KernelVersion kernelVersion, long txId, long timeWritten) {
        super(kernelVersion, TX_COMMIT);
        this.txId = txId;
        this.timeWritten = timeWritten;
    }

    public long getTxId() {
        return txId;
    }

    public long getTimeWritten() {
        return timeWritten;
    }

    public int getChecksum() {
        return 0;
    }

    @Override
    public String toString(Mask mask) {
        return "Commit[txId=" + getTxId() + ", " + timestamp(getTimeWritten()) + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntryCommit that = (LogEntryCommit) o;
        return txId == that.txId && timeWritten == that.timeWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(txId, timeWritten);
    }
}
