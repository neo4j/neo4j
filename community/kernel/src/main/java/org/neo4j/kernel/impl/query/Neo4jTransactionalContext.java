/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;

public class Neo4jTransactionalContext implements TransactionalContext
{
    private final GraphDatabaseQueryService graph;
    private final ThreadToStatementContextBridge txBridge;
    private final KernelTransaction.Type transactionType;
    private final AccessMode mode;
    private final DbmsOperations dbmsOperations;

    private InternalTransaction transaction;
    private Statement statement;
    private PropertyContainerLocker locker;

    private boolean isOpen = true;

    public Neo4jTransactionalContext( GraphDatabaseQueryService graph, InternalTransaction initialTransaction,
            Statement initialStatement, PropertyContainerLocker locker )
    {
        this.graph = graph;
        this.transaction = initialTransaction;
        this.transactionType = initialTransaction.transactionType();
        this.mode = initialTransaction.mode();
        this.statement = initialStatement;
        this.locker = locker;
        this.txBridge = graph.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        this.dbmsOperations = graph.getDependencyResolver().resolveDependency( DbmsOperations.class );
    }

    @Override
    public ReadOperations readOperations()
    {
        return statement.readOperations();
    }

    @Override
    public DbmsOperations dbmsOperations()
    {
        return dbmsOperations;
    }

    @Override
    public boolean isTopLevelTx()
    {
        return transaction.transactionType() == KernelTransaction.Type.implicit;
    }

    @Override
    public void close( boolean success )
    {
        if ( isOpen )
        {
            try
            {
                statement.close();

                if ( success )
                {
                    transaction.success();
                }
                else
                {
                    transaction.failure();
                }
                transaction.close();
            }
            finally
            {
                statement = null;
                transaction = null;
                isOpen = false;
            }
        }
    }

    @Override
    public void commitAndRestartTx()
    {
        transaction.success();
        transaction.close();

        transaction = graph.beginTransaction( transactionType, mode );
        statement = txBridge.get();
    }

    @Override
    public void cleanForReuse()
    {
        // close the old statement reference after the statement has been "upgraded"
        // to either a schema data or a schema statement, so that the locks are "handed over".
        statement.close();
        statement = txBridge.get();
    }

    @Override
    public TransactionalContext provideContext()
    {
        if ( isOpen )
        {
            return this;
        }
        else
        {
            InternalTransaction transaction =
                    graph.beginTransaction( transactionType, mode );
            Statement statement = txBridge.get();
            return new Neo4jTransactionalContext( graph, transaction, statement, locker );
        }
    }

    @Override
    public boolean isOpen()
    {
        return isOpen;
    }

    @Override
    public GraphDatabaseQueryService graph()
    {
        return graph;
    }

    @Override
    public Statement statement()
    {
        return statement;
    }

    @Override
    public TxStateHolder stateView()
    {
        return (KernelStatement) statement;
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer p )
    {
        return locker.exclusiveLock( () -> statement, p );
    }

    @Override
    public KernelTransaction.Revertable restrictCurrentTransaction( AccessMode accessMode )
    {
        return transaction.restrict( accessMode );
    }

    @Override
    public AccessMode accessMode()
    {
        return mode;
    }
}
