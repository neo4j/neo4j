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
package org.neo4j.kernel.impl.transaction;

import java.io.IOException;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.TransactionId;

public interface CommittedCommandBatchRepresentation {

    CommandBatch commandBatch();

    int serialize(LogEntryWriter<? extends WritableChannel> writer) throws IOException;

    int checksum();

    long timeWritten();

    long txId();

    boolean isRollback();

    long appendIndex();

    LogPosition previousBatchLogPosition();

    /**
     * @return an object containing only the meta-data about this command batch, w/o the commands themselves.
     */
    default BatchInformation batchInformation() {
        return new BatchInformation(
                txId(),
                commandBatch().kernelVersion(),
                checksum(),
                timeWritten(),
                commandBatch().consensusIndex(),
                appendIndex());
    }

    record BatchInformation(
            long txId,
            KernelVersion kernelVersion,
            int checksum,
            long timeWritten,
            long consensusIndex,
            long appendIndex) {
        public BatchInformation(TransactionId transactionId, long appendIndex) {
            this(
                    transactionId.id(),
                    transactionId.kernelVersion(),
                    transactionId.checksum(),
                    transactionId.commitTimestamp(),
                    transactionId.consensusIndex(),
                    appendIndex);
        }
    }
}
