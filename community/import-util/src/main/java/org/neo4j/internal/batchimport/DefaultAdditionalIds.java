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
package org.neo4j.internal.batchimport;

import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_VERSION;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

import org.neo4j.batchimport.api.AdditionalInitialIds;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.storageengine.api.TransactionIdStore;

public final class DefaultAdditionalIds {
    private DefaultAdditionalIds() {}

    /**
     * High ids of zero, useful when creating a completely new store with {@link BatchImporter}.
     */
    public static final AdditionalInitialIds EMPTY = new AdditionalInitialIds() {
        @Override
        public long lastCommittedTransactionId() {
            return TransactionIdStore.BASE_TX_ID;
        }

        @Override
        public int lastCommittedTransactionChecksum() {
            return TransactionIdStore.BASE_TX_CHECKSUM;
        }

        @Override
        public long lastCommittedTransactionLogVersion() {
            return BASE_TX_LOG_VERSION;
        }

        @Override
        public long lastCommittedTransactionLogByteOffset() {
            return BASE_TX_LOG_BYTE_OFFSET;
        }

        @Override
        public long checkpointLogVersion() {
            return INITIAL_LOG_VERSION;
        }

        @Override
        public long lastAppendIndex() {
            return BASE_APPEND_INDEX;
        }

        @Override
        public long lastCommittedTransactionAppendIndex() {
            return BASE_APPEND_INDEX;
        }
    };
}
