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

import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class Neo4jTransactionalContextFactory implements TransactionalContextFactory<Neo4jTransactionalContext>
{
    private final GraphDatabaseQueryService queryService;
    private final Supplier<Statement> statementSupplier;
    private final ThreadToStatementContextBridge txBridge;
    private final PropertyContainerLocker locker;
    private final DbmsOperations.Factory dbmsOpsFactory;

    public Neo4jTransactionalContextFactory( GraphDatabaseFacade.SPI spi, PropertyContainerLocker locker )
    {
        this.queryService = spi.queryService();
        this.locker = locker;
        DependencyResolver dependencyResolver = queryService.getDependencyResolver();
        this.txBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        this.dbmsOpsFactory = dependencyResolver.resolveDependency( DbmsOperations.Factory.class );
        this.statementSupplier = spi::currentStatement;
    }

    public Neo4jTransactionalContextFactory( GraphDatabaseQueryService queryService, PropertyContainerLocker locker )
    {
        this.queryService = queryService;
        this.locker = locker;
        DependencyResolver dependencyResolver = queryService.getDependencyResolver();
        this.txBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        this.dbmsOpsFactory = dependencyResolver.resolveDependency( DbmsOperations.Factory.class );
        this.statementSupplier = this.txBridge;
    }

    @Override
    public Neo4jTransactionalContext newContext(
        QuerySource querySource,
        InternalTransaction tx,
        String queryText,
        Map<String,Object> queryParameters
    )
    {
        Statement statement = txBridge.get();
        QuerySource querySourceWithUserName = querySource.append( tx.mode().name() );
        ExecutingQuery executingQuery = statement.queryRegistration().startQueryExecution(
            querySourceWithUserName, queryText, queryParameters
        );
        return new Neo4jTransactionalContext(
                queryService,
                tx,
                tx.transactionType(),
                tx.mode(),
                statementSupplier.get(),
                executingQuery,
                locker,
                txBridge,
                dbmsOpsFactory,
                queryService.getDependencyResolver().resolveDependency( Guard.class )
        );
    }
}
