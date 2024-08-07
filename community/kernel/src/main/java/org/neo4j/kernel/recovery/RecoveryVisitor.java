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

import static org.neo4j.io.IOUtils.closeAllUnchecked;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransaction;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;

final class RecoveryVisitor implements RecoveryApplier {
    private final StorageEngine storageEngine;
    private final TransactionApplicationMode mode;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;

    RecoveryVisitor(
            StorageEngine storageEngine,
            TransactionApplicationMode mode,
            CursorContextFactory contextFactory,
            String tracerTag) {
        this.storageEngine = storageEngine;
        this.mode = mode;
        this.cursorContext = contextFactory.create(tracerTag);
        this.storeCursors = storageEngine.createStorageCursors(cursorContext);
    }

    @Override
    public boolean visit(CommittedCommandBatchRepresentation batch) throws Exception {
        StorageEngineTransaction storageEngineTransaction = commandToApply(batch);
        storageEngine.apply(storageEngineTransaction, mode);
        return false;
    }

    private StorageEngineTransaction commandToApply(CommittedCommandBatchRepresentation batch) {
        var commandsToApply = batch instanceof CompleteBatchRepresentation
                ? new CompleteTransaction(batch, cursorContext, storeCursors)
                : new ChunkedTransaction(batch, cursorContext, storeCursors);
        cursorContext.getVersionContext().initWrite(commandsToApply.transactionId());
        return commandsToApply;
    }

    @Override
    public void close() {
        closeAllUnchecked(storeCursors, cursorContext);
    }
}
