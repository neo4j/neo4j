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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;

public class Neo4jTransactionalContextFactory implements TransactionalContextFactory
{
    private final Neo4jTransactionalContext.Dependencies dependencies;

    @Deprecated
    public Neo4jTransactionalContextFactory( GraphDatabaseQueryService queryService, PropertyContainerLocker locker )
    {
        this( new Neo4jTransactionalContext.Dependencies() {
            private final Guard guard;
            private final ThreadToStatementContextBridge txBridge;

            {
                DependencyResolver resolver = queryService.getDependencyResolver();
                guard = resolver.resolveDependency( Guard.class );
                txBridge = resolver.resolveDependency( ThreadToStatementContextBridge.class );
            }

            @Override
            public GraphDatabaseQueryService queryService()
            {
                return queryService;
            }

            @Override
            public Statement currentStatement()
            {
                return txBridge.get();
            }

            @Override
            public void check( KernelStatement statement )
            {
                guard.check( statement );
            }

            @Override
            public ThreadToStatementContextBridge txBridge()
            {
                return txBridge;
            }

            @Override
            public PropertyContainerLocker locker()
            {
                return locker;
            }
        } );
    }

    public Neo4jTransactionalContextFactory( Neo4jTransactionalContext.Dependencies dependencies )
    {
        this.dependencies = dependencies;
    }

    @Override
    public Neo4jTransactionalContext newContext(
        QuerySource querySource,
        InternalTransaction tx,
        String queryText,
        Map<String,Object> queryParameters
    )
    {
        Statement statement = dependencies.currentStatement();
        QuerySource querySourceWithUserName = querySource.append( tx.securityContext().subject().username() );
        ExecutingQuery executingQuery = statement.queryRegistration().startQueryExecution(
            querySourceWithUserName, queryText, queryParameters
        );
        return new Neo4jTransactionalContext( dependencies, tx, statement, executingQuery );
    }
}
