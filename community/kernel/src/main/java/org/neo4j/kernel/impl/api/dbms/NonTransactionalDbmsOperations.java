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
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.values.AnyValue;

public class NonTransactionalDbmsOperations implements DbmsOperations
{

    private final Procedures procedures;

    public NonTransactionalDbmsOperations( Procedures procedures )
    {
        this.procedures = procedures;
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallDbms(
            QualifiedName name,
            Object[] input,
            SecurityContext securityContext,
            ResourceTracker resourceTracker
    ) throws ProcedureException
    {
        BasicContext ctx = new BasicContext();
        ctx.put( Context.SECURITY_CONTEXT, securityContext );
        return procedures.callProcedure( ctx, name, input, resourceTracker );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallDbms(
            int id,
            Object[] input,
            SecurityContext securityContext,
            ResourceTracker resourceTracker
    ) throws ProcedureException
    {
        BasicContext ctx = new BasicContext();
        ctx.put( Context.SECURITY_CONTEXT, securityContext );
        return procedures.callProcedure( ctx, id, input, resourceTracker );
    }

    @Override
    public AnyValue functionCallDbms(
            QualifiedName name,
            AnyValue[] input,
            SecurityContext securityContext
    ) throws ProcedureException
    {
        BasicContext ctx = new BasicContext();
        ctx.put( Context.SECURITY_CONTEXT, securityContext );
        return procedures.callFunction( ctx, name, input );
    }
}
