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
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;

public class NonTransactionalDbmsOperations implements DbmsOperations
{
    private final GlobalProcedures globalProcedures;

    public NonTransactionalDbmsOperations( GlobalProcedures globalProcedures )
    {
        this.globalProcedures = globalProcedures;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallDbms( int id, AnyValue[] input, DependencyResolver dependencyResolver,
            SecurityContext securityContext, ResourceTracker resourceTracker, ValueMapper<Object> valueMapper ) throws ProcedureException
    {
        Context ctx = createContext( securityContext, dependencyResolver, valueMapper );
        return globalProcedures.callProcedure( ctx, id, input, resourceTracker );
    }

    private static Context createContext( SecurityContext securityContext, DependencyResolver dependencyResolver, ValueMapper<Object> valueMapper )
    {
        return buildContext( dependencyResolver, valueMapper )
                .withSecurityContext( securityContext ).context();
    }
}
