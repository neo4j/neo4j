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
package org.neo4j.kernel.impl.query;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.values.ElementIdMapper;

public class DelegatingTransactionalContext implements TransactionalContext {
    private final TransactionalContext inner;

    public DelegatingTransactionalContext(TransactionalContext inner) {
        this.inner = inner;
    }

    @Override
    public ExecutingQuery executingQuery() {
        return inner.executingQuery();
    }

    @Override
    public KernelTransaction kernelTransaction() {
        return inner.kernelTransaction();
    }

    @Override
    public InternalTransaction transaction() {
        return inner.transaction();
    }

    @Override
    public boolean isTopLevelTx() {
        return inner.isTopLevelTx();
    }

    @Override
    public ConstituentTransactionFactory constituentTransactionFactory() {
        return inner.constituentTransactionFactory();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public void commit() {
        inner.commit();
    }

    @Override
    public void rollback() {
        inner.rollback();
    }

    @Override
    public void terminate() {
        inner.terminate();
    }

    @Override
    public long commitAndRestartTx() {
        return inner.commitAndRestartTx();
    }

    @Override
    public TransactionalContext contextWithNewTransaction() {
        return inner.contextWithNewTransaction();
    }

    @Override
    public TransactionalContext getOrBeginNewIfClosed() {
        return inner.getOrBeginNewIfClosed();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public GraphDatabaseQueryService graph() {
        return inner.graph();
    }

    @Override
    public NamedDatabaseId databaseId() {
        return inner.databaseId();
    }

    @Override
    public Statement statement() {
        return inner.statement();
    }

    @Override
    public SecurityContext securityContext() {
        return inner.securityContext();
    }

    @Override
    public StatisticProvider kernelStatisticProvider() {
        return inner.kernelStatisticProvider();
    }

    @Override
    public KernelTransaction.Revertable restrictCurrentTransaction(SecurityContext context) {
        return inner.restrictCurrentTransaction(context);
    }

    @Override
    public ResourceTracker resourceTracker() {
        return inner.resourceTracker();
    }

    @Override
    public ElementIdMapper elementIdMapper() {
        return inner.elementIdMapper();
    }

    @Override
    public QueryExecutionConfiguration queryExecutingConfiguration() {
        return inner.queryExecutingConfiguration();
    }

    @Override
    public DatabaseMode databaseMode() {
        return inner.databaseMode();
    }
}
