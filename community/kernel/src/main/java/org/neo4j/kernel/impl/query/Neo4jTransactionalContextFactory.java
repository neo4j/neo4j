/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.query;

import java.util.function.Supplier;

import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.function.Suppliers.lazySingleton;

public class Neo4jTransactionalContextFactory implements TransactionalContextFactory
{
    private final Supplier<Statement> statementSupplier;
    private final Neo4jTransactionalContext.Creator contextCreator;

    public static TransactionalContextFactory create( EmbeddedProxySPI proxySpi, Supplier<GraphDatabaseQueryService> queryServiceSupplier,
            KernelTransactionFactory transactionFactory, ThreadToStatementContextBridge txBridge )
    {
        Supplier<GraphDatabaseQueryService> queryService = lazySingleton( queryServiceSupplier );
        Neo4jTransactionalContext.Creator contextCreator =
                ( tx, initialStatement, executingQuery ) -> new Neo4jTransactionalContext( queryService.get(), txBridge, tx, initialStatement, executingQuery,
                        new DefaultValueMapper( proxySpi ), transactionFactory );
        Supplier<Statement> statementSupplier = () -> proxySpi.kernelTransaction().acquireStatement();
        return new Neo4jTransactionalContextFactory( statementSupplier, contextCreator );
    }

    @Deprecated
    public static TransactionalContextFactory create( GraphDatabaseQueryService queryService )
    {
        var resolver = queryService.getDependencyResolver();
        var txBridge = resolver.resolveDependency( ThreadToStatementContextBridge.class );
        var proxySpi = resolver.resolveDependency( EmbeddedProxySPI.class );
        var transactionFactory = resolver.resolveDependency( KernelTransactionFactory.class );
        Neo4jTransactionalContext.Creator contextCreator =
                ( tx, initialStatement, executingQuery ) ->
                        new Neo4jTransactionalContext(
                                queryService,
                                txBridge,
                                tx,
                                initialStatement,
                                executingQuery,
                                new DefaultValueMapper( proxySpi ),
                                transactionFactory
                        );
        Supplier<Statement> statementSupplier = () -> proxySpi.kernelTransaction().acquireStatement();
        return new Neo4jTransactionalContextFactory( statementSupplier, contextCreator );
    }

    // Please use the factory methods above to actually construct an instance
    private Neo4jTransactionalContextFactory( Supplier<Statement> statementSupplier, Neo4jTransactionalContext.Creator contextCreator )
    {
        this.statementSupplier = statementSupplier;
        this.contextCreator = contextCreator;
    }

    @Override
    public final Neo4jTransactionalContext newContext( InternalTransaction tx, String queryText, MapValue queryParameters )
    {
        Statement initialStatement = statementSupplier.get();
        var executingQuery = initialStatement.queryRegistration().startQueryExecution( queryText, queryParameters );
        return contextCreator.create( tx, initialStatement, executingQuery );
    }
}
