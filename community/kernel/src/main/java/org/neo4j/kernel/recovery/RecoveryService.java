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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public interface RecoveryService {
    CommandBatchCursor getCommandBatches(long appendIndex) throws IOException;

    CommandBatchCursor getCommandBatches(LogPosition recoveryFromPosition) throws IOException;

    CommandBatchCursor getCommandBatchesInReverseOrder(LogPosition recoveryFromPosition) throws IOException;

    RecoveryStartInformation getRecoveryStartInformation() throws IOException;

    RecoveryApplier getRecoveryApplier(
            TransactionApplicationMode mode, CursorContextFactory contextFactory, String tracerTag) throws Exception;

    void transactionsRecovered(
            CommittedCommandBatch.BatchInformation highestTransactionRecoveredBatch,
            AppendIndexProvider recoverAppendIndexProvider,
            LogPosition lastTransactionPosition,
            LogPosition positionAfterLastRecoveredTransaction,
            LogPosition checkpointPosition,
            boolean missingLogs,
            CursorContext cursorContext);
}
