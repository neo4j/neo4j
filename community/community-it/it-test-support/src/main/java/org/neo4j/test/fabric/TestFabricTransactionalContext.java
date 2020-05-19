/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test.fabric;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.values.ValueMapper;

public class TestFabricTransactionalContext implements TransactionalContext
{
    private final InternalTransaction transaction;

    TestFabricTransactionalContext( InternalTransaction transaction )
    {
        this.transaction = transaction;
    }

    private UnsupportedOperationException failure()
    {
        return new UnsupportedOperationException( "Not implemented" );
    }

    @Override
    public ValueMapper<Object> valueMapper()
    {
        return new TestFabricValueMapper();
    }

    @Override
    public ExecutingQuery executingQuery()
    {
        throw failure();
    }

    @Override
    public DbmsOperations dbmsOperations()
    {
        throw failure();
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        return transaction.kernelTransaction();
    }

    @Override
    public InternalTransaction transaction()
    {
        return transaction;
    }

    @Override
    public boolean isTopLevelTx()
    {
        throw failure();
    }

    @Override
    public void close()
    {
        throw failure();
    }

    @Override
    public void rollback()
    {
        throw failure();
    }

    @Override
    public void terminate()
    {
        throw failure();
    }

    @Override
    public void commitAndRestartTx()
    {
        throw failure();
    }

    @Override
    public TransactionalContext getOrBeginNewIfClosed()
    {
        throw failure();
    }

    @Override
    public boolean isOpen()
    {
        throw failure();
    }

    @Override
    public GraphDatabaseQueryService graph()
    {
        throw failure();
    }

    @Override
    public NamedDatabaseId databaseId()
    {
        throw failure();
    }

    @Override
    public Statement statement()
    {
        throw failure();
    }

    @Override
    public void check()
    {
        throw failure();
    }

    @Override
    public SecurityContext securityContext()
    {
        throw failure();
    }

    @Override
    public StatisticProvider kernelStatisticProvider()
    {
        throw failure();
    }

    @Override
    public KernelTransaction.Revertable restrictCurrentTransaction( SecurityContext context )
    {
        throw failure();
    }

    @Override
    public ResourceTracker resourceTracker()
    {
        throw failure();
    }
}
