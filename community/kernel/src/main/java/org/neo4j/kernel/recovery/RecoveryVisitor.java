/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.recovery;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.impl.transaction.log.Commitment.NO_COMMITMENT;

final class RecoveryVisitor implements RecoveryApplier
{
    private final StorageEngine storageEngine;
    private final TransactionApplicationMode mode;
    private final CursorContext cursorContext;

    RecoveryVisitor( StorageEngine storageEngine, TransactionApplicationMode mode, PageCacheTracer cacheTracer, String tracerTag )
    {
        this.storageEngine = storageEngine;
        this.mode = mode;
        this.cursorContext = new CursorContext( cacheTracer.createPageCursorTracer( tracerTag ) );
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation transaction ) throws Exception
    {
        var txRepresentation = transaction.getTransactionRepresentation();
        var txId = transaction.getCommitEntry().getTxId();
        var tx = new TransactionToApply( txRepresentation, txId, cursorContext );
        tx.commitment( NO_COMMITMENT, txId );
        tx.logPosition( transaction.getStartEntry().getStartPosition() );
        storageEngine.apply( tx, mode );
        return false;
    }

    @Override
    public void close()
    {
        cursorContext.close();
    }
}
