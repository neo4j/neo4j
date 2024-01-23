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
package org.neo4j.fabric.executor;

import java.util.List;
import org.neo4j.fabric.bookmark.LocalBookmark;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.fabric.transaction.parent.CompoundTransaction;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.api.exceptions.Status;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AutocommitLocalStatementResult implements StatementResult, CompoundTransaction.AutocommitQuery {
    private final StatementResult result;
    private final FabricKernelTransaction transaction;
    private final TransactionBookmarkManager bookmarkManager;
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private final Location.Local location;

    public AutocommitLocalStatementResult(
            StatementResult result,
            FabricKernelTransaction transaction,
            TransactionBookmarkManager bookmarkManager,
            LocalGraphTransactionIdTracker transactionIdTracker,
            Location.Local location) {
        this.result = result;
        this.transaction = transaction;
        this.bookmarkManager = bookmarkManager;
        this.transactionIdTracker = transactionIdTracker;
        this.location = location;
    }

    @Override
    public List<String> columns() {
        return result.columns();
    }

    @Override
    public Flux<Record> records() {
        return result.records().doOnComplete(this::doCommit);
    }

    @Override
    public Mono<Summary> summary() {
        return result.summary();
    }

    @Override
    public Mono<QueryExecutionType> executionType() {
        return result.executionType();
    }

    @Override
    public void terminate(Status reason) {
        transaction.terminate(reason);
    }

    private void doCommit() {
        long transactionId = transactionIdTracker.getTransactionId(location);
        transaction.commit();
        bookmarkManager.localTransactionCommitted(location, new LocalBookmark(transactionId));
    }
}
