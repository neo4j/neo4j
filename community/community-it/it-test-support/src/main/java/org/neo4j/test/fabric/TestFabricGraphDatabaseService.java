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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.bolt.BoltFabricDatabaseService;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.util.Objects.requireNonNull;

public class TestFabricGraphDatabaseService extends GraphDatabaseFacade
{
    final Supplier<BoltGraphDatabaseServiceSPI> boltFabricDatabaseServiceSupplier;
    final Config config;

    public TestFabricGraphDatabaseService( GraphDatabaseFacade baseDb,
                                           Config config,
                                           Supplier<BoltGraphDatabaseServiceSPI> boltFabricDatabaseServiceSupplier )
    {
        super( baseDb, Function.identity() );
        this.boltFabricDatabaseServiceSupplier = boltFabricDatabaseServiceSupplier;
        this.config = requireNonNull( config );
    }

    @Override
    protected InternalTransaction beginTransactionInternal( KernelTransaction.Type type,
                                                            LoginContext loginContext,
                                                            ClientConnectionInfo connectionInfo,
                                                            long timeoutMillis,
                                                            Consumer<Status> terminationCallback,
                                                            TransactionExceptionMapper transactionExceptionMapper )
    {

        var databaseService = boltFabricDatabaseServiceSupplier.get();
        var boltTransaction = databaseService.beginTransaction( type, loginContext, connectionInfo, List.of(),
                                                                          Duration.ofMillis( timeoutMillis ), AccessMode.WRITE,
                                                                          Map.of(),
                                                                          new RoutingContext( false, Map.of() ) );
        var internalTransaction = forceKernelTxCreation( boltTransaction );
        return new TestFabricTransaction( boltTransaction, internalTransaction );
    }

    private InternalTransaction forceKernelTxCreation( BoltTransaction boltTransaction )
    {
        FabricExecutor fabricExecutor = getDependencyResolver().resolveDependency( FabricExecutor.class );
        FabricTransaction fabricTransaction = ((BoltFabricDatabaseService.BoltTransactionImpl) boltTransaction).getFabricTransaction();
        return fabricExecutor.forceKernelTxCreation( fabricTransaction );
    }
}
