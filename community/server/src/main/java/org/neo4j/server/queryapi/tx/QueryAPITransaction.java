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
package org.neo4j.server.queryapi.tx;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.summary.ResultSummary;

public class QueryAPITransaction implements Transaction {

    private final String transactionId;
    private final org.neo4j.driver.Transaction delegateTransaction;
    private final Session session;
    private final AuthToken authToken;
    private final String databaseName;
    private final ReentrantLock lock = new ReentrantLock();
    private Result currentResult;
    private Instant transactionExpiry;
    private final Duration timeoutExtensionDuration;

    public QueryAPITransaction(
            String transactionId,
            org.neo4j.driver.Transaction delegateTransaction,
            Session session,
            AuthToken authToken,
            String databaseName,
            Instant transactionExpiry,
            Duration timeoutExtensionDuration) {
        this.transactionId = transactionId;
        this.delegateTransaction = delegateTransaction;
        this.session = session;
        this.authToken = authToken;
        this.databaseName = databaseName;
        this.transactionExpiry = transactionExpiry;
        this.timeoutExtensionDuration = timeoutExtensionDuration;
    }

    @Override
    public void runQuery(String statement, Map<String, Object> parameters) {
        this.currentResult = delegateTransaction.run(statement, parameters);
    }

    @Override
    public Result retrieveResults() {
        return currentResult;
    }

    @Override
    public Set<Bookmark> commit() {
        delegateTransaction.commit();
        return session.lastBookmarks();
    }

    @Override
    public void rollback() {
        delegateTransaction.rollback();
    }

    @Override
    public ResultSummary resultSummary() {
        return currentResult.consume();
    }

    @Override
    public boolean isOpen() {
        return delegateTransaction.isOpen();
    }

    @Override
    public void close() {
        delegateTransaction.close();
        session.close();
    }

    @Override
    public boolean tryAcquire() {
        return lock.tryLock();
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit timeUnit) {
        try {
            return tryAcquire() || lock.tryLock(timeout, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void release() {
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    @Override
    public Instant expiresAt() {
        return transactionExpiry;
    }

    @Override
    public String id() {
        return transactionId;
    }

    @Override
    public void extendTimeout() {
        this.transactionExpiry =
                Instant.now().truncatedTo(ChronoUnit.SECONDS).plusSeconds(timeoutExtensionDuration.getSeconds());
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public AuthToken authToken() {
        return authToken;
    }
}
