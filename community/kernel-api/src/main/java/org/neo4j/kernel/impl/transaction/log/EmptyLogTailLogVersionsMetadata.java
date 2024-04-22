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
package org.neo4j.kernel.impl.transaction.log;

import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

public class EmptyLogTailLogVersionsMetadata implements LogTailLogVersionsMetadata {
    static final LogPosition START_POSITION = new LogPosition(INITIAL_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET);

    @Override
    public boolean isRecoveryRequired() {
        return false;
    }

    @Override
    public long getCheckpointLogVersion() {
        return INITIAL_LOG_VERSION;
    }

    @Override
    public long getLogVersion() {
        return INITIAL_LOG_VERSION;
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return TransactionIdStore.BASE_TRANSACTION_ID;
    }

    @Override
    public LogPosition getLastTransactionLogPosition() {
        return START_POSITION;
    }

    @Override
    public long getLastCheckpointedAppendIndex() {
        return BASE_APPEND_INDEX;
    }
}
