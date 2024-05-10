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
package org.neo4j.storageengine.api;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Represents a commitment that is result of applying batch of commands to transaction logs
 * means. As a transaction is carried through the commit process this commitment is updated
 * when {@link #publishAsCommitted(long, long)} committed (which happens when appending to log), but also
 * when {@link #publishAsClosed()} closing.
 */
public interface Commitment {
    Commitment NO_COMMITMENT = new Commitment() {

        @Override
        public void commit(
                long transactionId,
                long appendIndex,
                KernelVersion kernelVersion,
                LogPosition logPositionAfterCommit,
                int checksum,
                long consensusIndex) {}

        @Override
        public void publishAsCommitted(long transactionCommitTimestamp, long firstAppendIndex) {}

        @Override
        public void publishAsClosed() {}
    };

    void commit(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            LogPosition logPositionAfterCommit,
            int checksum,
            long consensusIndex);

    /**
     * Marks the transaction as committed and makes this fact public.
     */
    void publishAsCommitted(long transactionCommitTimestamp, long firstAppendIndex);

    /**
     * Marks the transaction as closed and makes this fact public.
     */
    void publishAsClosed();
}
