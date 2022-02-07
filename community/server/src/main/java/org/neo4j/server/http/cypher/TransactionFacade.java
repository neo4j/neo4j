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
package org.neo4j.server.http.cypher;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.time.SystemNanoClock;

/**
 * Transactional actions contains the business logic for executing statements against Neo4j across long-running
 * transactions.
 * <p>
 * The idiom for the public methods here is:
 * <p>
 * response.begin()
 * try {
 * // Do internal calls, saving errors into a common error list
 * } catch ( Neo4jError e )
 * {
 * errors.add(e);
 * } finally
 * {
 * response.finish(errors)
 * }
 * <p>
 * This is done to ensure we stick to the contract of the response handler, which is important, because if we skimp on
 * it, clients may be left waiting for results that never arrive.
 */
class TransactionFacade
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( TransactionFacade.class );

    private final String databaseName;
    private final QueryExecutionEngine engine;
    private final TransactionRegistry registry;
    private final TransactionManager transactionManager;
    private final LogProvider logProvider;
    private final BoltGraphDatabaseManagementServiceSPI boltSPI;
    private final AuthManager authManager;
    private final boolean readByDefault;

    TransactionFacade( String databaseName, QueryExecutionEngine engine, TransactionRegistry registry,
                       TransactionManager transactionManager, LogProvider logProvider, BoltGraphDatabaseManagementServiceSPI boltSPI,
                       AuthManager authManager, boolean readByDefault )
    {
        this.databaseName = databaseName;
        this.engine = engine;
        this.registry = registry;
        this.transactionManager = transactionManager;
        this.logProvider = logProvider;
        this.boltSPI = boltSPI;
        this.authManager = authManager;
        this.readByDefault = readByDefault;
    }

    TransactionHandle newTransactionHandle( TransactionUriScheme uriScheme, boolean implicitTransaction,
                                            LoginContext loginContext, ClientConnectionInfo clientConnectionInfo,
                                            MemoryTracker memoryTracker, long customTransactionTimeout,
                                            SystemNanoClock clock )
    {
        memoryTracker.allocateHeap( TransactionHandle.SHALLOW_SIZE );

        return new TransactionHandle( databaseName, engine, registry, uriScheme, implicitTransaction,
                                      loginContext, clientConnectionInfo, customTransactionTimeout, transactionManager, logProvider, boltSPI, memoryTracker,
                                      authManager, clock, readByDefault );
    }

    TransactionHandle newTransactionHandle( TransactionUriScheme uriScheme, boolean implicitTransaction,
                                            LoginContext loginContext, ClientConnectionInfo clientConnectionInfo,
                                            MemoryTracker memoryTracker, long customTransactionTimeout,
                                            SystemNanoClock clock, boolean isReadOnlyTransaction )
    {
        memoryTracker.allocateHeap( TransactionHandle.SHALLOW_SIZE );

        return new TransactionHandle( databaseName, engine, registry, uriScheme, implicitTransaction,
                                      loginContext, clientConnectionInfo, customTransactionTimeout, transactionManager, logProvider, boltSPI, memoryTracker,
                                      authManager, clock, isReadOnlyTransaction );
    }

    TransactionHandle findTransactionHandle( long txId ) throws TransactionLifecycleException
    {
        return registry.acquire( txId );
    }

    TransactionHandle terminate( long txId ) throws TransactionLifecycleException
    {
        return registry.terminate( txId );
    }
}
