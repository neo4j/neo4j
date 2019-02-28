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
package org.neo4j.kernel.builtinprocs.routing;

import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;

import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.builtinprocs.routing.ParameterNames.CONTEXT;
import static org.neo4j.kernel.builtinprocs.routing.ParameterNames.SERVERS;
import static org.neo4j.kernel.builtinprocs.routing.ParameterNames.TTL;

public abstract class BaseGetRoutingTableProcedure implements CallableProcedure
{
    private static final String NAME = "getRoutingTable";

    private final ProcedureSignature signature;

    protected BaseGetRoutingTableProcedure( List<String> namespace )
    {
        signature = procedureSignature( new QualifiedName( namespace, NAME ) )
                .in( CONTEXT.parameterName(), Neo4jTypes.NTMap )
                .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                .out( SERVERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                .mode( Mode.DBMS )
                .description( description() )
                .build();
    }

    @Override
    public final ProcedureSignature signature()
    {
        return signature;
    }

    @Override
    public final RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        RoutingResult result = invoke( input );
        return RawIterator.<AnyValue[],ProcedureException>of( RoutingResultFormat.build( result ) );
    }

    protected abstract String description();

    protected abstract RoutingResult invoke( AnyValue[] input ) throws ProcedureException;
}
