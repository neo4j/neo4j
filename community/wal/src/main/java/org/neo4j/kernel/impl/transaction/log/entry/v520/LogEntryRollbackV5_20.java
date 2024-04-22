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

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryRollback;
import org.neo4j.string.Mask;

public class LogEntryRollbackV5_20 extends LogEntryRollback {
    private final long appendIndex;

    public LogEntryRollbackV5_20(
            KernelVersion kernelVersion, long transactionId, long appendIndex, long timeWritten, int checksum) {
        super(kernelVersion, transactionId, timeWritten, checksum);
        this.appendIndex = appendIndex;
    }

    @Override
    public long getAppendIndex() {
        return appendIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryRollbackV5_20{" + "transactionId="
                + transactionId + ", timeWritten="
                + timeWritten + ", checksum="
                + checksum + ", appendIndex=" + appendIndex + '}';
    }
}
