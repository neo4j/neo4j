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
package org.neo4j.server.database;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.rest.web.HttpConnectionInfoFactory;

import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.server.web.HttpHeaderUtils.getTransactionTimeout;

public class CypherExecutor extends LifecycleAdapter
{
    private final Database database;
    private final Log log;
    private ExecutionEngine executionEngine;
    private TransactionalContextFactory contextFactory;

    private static final PropertyContainerLocker locker = new PropertyContainerLocker();
    private GraphDatabaseQueryService service;

    public CypherExecutor( Database database, LogProvider logProvider )
    {
        this.database = database;
        log = logProvider.getLog( getClass() );
    }

    public ExecutionEngine getExecutionEngine()
    {
        return executionEngine;
    }

    @Override
    public void start()
    {
        DependencyResolver resolver = database.getGraph().getDependencyResolver();
        this.executionEngine = (ExecutionEngine) resolver.resolveDependency( QueryExecutionEngine.class );
        this.service = resolver.resolveDependency( GraphDatabaseQueryService.class );
        this.contextFactory = Neo4jTransactionalContextFactory.create( this.service, locker );
    }

    @Override
    public void stop()
    {
        this.executionEngine = null;
        this.contextFactory = null;
    }

    public TransactionalContext createTransactionContext( String query, Map<String, Object> parameters,
            HttpServletRequest request )
    {
        InternalTransaction tx = getInternalTransaction( request );
        return contextFactory.newContext( HttpConnectionInfoFactory.create( request ), tx, query, asMapValue( parameters ));
    }

    private InternalTransaction getInternalTransaction( HttpServletRequest request )
    {
        long customTimeout = getTransactionTimeout( request, log );
        return customTimeout > GraphDatabaseSettings.UNSPECIFIED_TIMEOUT ?
           beginCustomTransaction( customTimeout ) : beginDefaultTransaction();
    }

    private InternalTransaction beginCustomTransaction( long customTimeout )
    {
        return service.beginTransaction(
            KernelTransaction.Type.implicit, AUTH_DISABLED, customTimeout, TimeUnit.MILLISECONDS
        );
    }

    private InternalTransaction beginDefaultTransaction()
    {
        return service.beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
    }
}
