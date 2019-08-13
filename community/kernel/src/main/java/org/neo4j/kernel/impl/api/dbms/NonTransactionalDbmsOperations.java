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
package org.neo4j.kernel.impl.api.dbms;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.kernel.api.proc.Context.DATABASE_API;
import static org.neo4j.kernel.api.proc.Context.DEPENDENCY_RESOLVER;
import static org.neo4j.kernel.api.proc.Context.PROCEDURE_CALL_CONTEXT;
import static org.neo4j.kernel.api.proc.Context.SECURITY_CONTEXT;

public class NonTransactionalDbmsOperations implements DbmsOperations
{

    private final Procedures procedures;

    public NonTransactionalDbmsOperations( Procedures procedures )
    {
        this.procedures = procedures;
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallDbms( QualifiedName name, Object[] input, DependencyResolver dependencyResolver,
            SecurityContext securityContext, ResourceTracker resourceTracker, ProcedureCallContext procedureCallContext ) throws ProcedureException
    {
        BasicContext ctx = createContext( securityContext, dependencyResolver, procedureCallContext );
        return procedures.callProcedure( ctx, name, input, resourceTracker );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallDbms( int id, Object[] input, DependencyResolver dependencyResolver,
            SecurityContext securityContext, ResourceTracker resourceTracker, ProcedureCallContext procedureCallContext ) throws ProcedureException
    {
        BasicContext ctx = createContext( securityContext, dependencyResolver, procedureCallContext );
        return procedures.callProcedure( ctx, id, input, resourceTracker );
    }

    private static BasicContext createContext( SecurityContext securityContext, DependencyResolver dependencyResolver,
            ProcedureCallContext procedureCallContext )
    {
        BasicContext ctx = new BasicContext();
        ctx.put( SECURITY_CONTEXT, securityContext );
        ctx.put( PROCEDURE_CALL_CONTEXT, procedureCallContext );
        ctx.put( DEPENDENCY_RESOLVER, dependencyResolver );
        ctx.put( DATABASE_API, dependencyResolver.resolveDependency( GraphDatabaseAPI.class ) );
        return ctx;
    }
}
