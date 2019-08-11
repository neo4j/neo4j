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
package org.neo4j.cypher.internal.javacompat;

import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class SystemDatabaseInnerAccessor implements Supplier<GraphDatabaseQueryService>
{
    private GraphDatabaseFacade systemDb;
    private SystemDatabaseInnerEngine engine;
    private TransactionalContextFactory contextFactory;

    SystemDatabaseInnerAccessor( GraphDatabaseFacade systemDb, SystemDatabaseInnerEngine engine )
    {
        this.systemDb = systemDb;
        this.engine = engine;
        var dependencyResolver = this.systemDb.getDependencyResolver();
        var transactionFactory = dependencyResolver.resolveDependency( KernelTransactionFactory.class );
        var txBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        this.contextFactory = Neo4jTransactionalContextFactory.create( this.systemDb, this, transactionFactory, txBridge );
    }

    public InternalTransaction beginTx()
    {
        return systemDb.beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED );
    }

    public QueryExecution execute(  InternalTransaction transaction, String query, Map<String,Object> params, QuerySubscriber subscriber )
    {
        try
        {
            MapValue parameters = ValueUtils.asParameterMapValue( params );
            TransactionalContext context = contextFactory.newContext( transaction, query, parameters );
            return this.engine.execute( query, parameters, context, subscriber );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public GraphDatabaseQueryService get()
    {
        return systemDb.getDependencyResolver().resolveDependency( GraphDatabaseQueryService.class );
    }

    /**
     * The multidatabase system needs different access to the query execution engine for the system database when compared to normal databases.
     * User queries executed against the system database will only be understood if they are system administration commands. However, internally
     * these commands are converted into normal Cypher commands, which are also executed against the system database. To support this the
     * system database has two execution engines, the normal accessible from the outside only supports administration commands and no graph commands,
     * while the inner one which does understand graph commands is only available to key internal infrastructure like the security infrastructure.
     */
    public interface SystemDatabaseInnerEngine
    {
        QueryExecution execute( String query, MapValue parameters, TransactionalContext context,
                QuerySubscriber subscriber ) throws QueryExecutionKernelException;
    }
}
