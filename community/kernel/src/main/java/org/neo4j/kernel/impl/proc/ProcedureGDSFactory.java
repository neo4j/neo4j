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
package org.neo4j.kernel.impl.proc;

import java.io.File;
import java.net.URL;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.legacyindex.AutoIndexing;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.StoreId;

public class ProcedureGDSFactory implements ThrowingFunction<Context,GraphDatabaseService,ProcedureException>
{
    private Config config;
    private final File storeDir;
    private final DependencyResolver resolver;
    private final Supplier<StoreId> storeId;
    private final Supplier<QueryExecutionEngine> queryExecutor;
    private final CoreAPIAvailabilityGuard availability;
    private final ThrowingFunction<URL, URL, URLAccessValidationError> urlValidator;
    private final Guard guard;
    private final ThreadToStatementContextBridge txBridge;

    public ProcedureGDSFactory( Config config,
                                File storeDir,
                                DependencyResolver resolver,
                                Supplier<StoreId> storeId,
                                Supplier<QueryExecutionEngine> queryExecutor,
                                CoreAPIAvailabilityGuard availability,
                                URLAccessRule urlAccessRule )
    {
        this.config = config;
        this.storeDir = storeDir;
        this.resolver = resolver;
        this.storeId = storeId;
        this.queryExecutor = queryExecutor;
        this.availability = availability;
        this.urlValidator = url -> urlAccessRule.validate( config, url );
        this.guard = resolver.resolveDependency( Guard.class );
        this.txBridge = resolver.resolveDependency( ThreadToStatementContextBridge.class );
    }

    @Override
    public GraphDatabaseService apply( Context context ) throws ProcedureException
    {
        KernelTransaction transaction = context.get( Context.KERNEL_TRANSACTION );
        Thread owningThread = context.get( Context.THREAD );
        GraphDatabaseFacade facade = new GraphDatabaseFacade();
        facade.init(
            new ProcedureGDBFacadeSPI(
                owningThread,
                transaction,
                queryExecutor,
                storeDir,
                resolver,
                AutoIndexing.UNSUPPORTED,
                storeId,
                availability,
                urlValidator
            ),
            guard,
            txBridge,
            config
        );
        return facade;
    }
}
