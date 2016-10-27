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
package org.neo4j.kernel.builtinprocs;

import java.util.stream.Stream;

import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class BuiltInDbmsProcedures
{
    @Context
    public GraphDatabaseAPI graph;

    @Description( "List all procedures in the DBMS." )
    @Procedure( name = "dbms.procedures", mode = DBMS )
    public Stream<ProcedureResult> listProcedures()
    {
        return graph.getDependencyResolver().resolveDependency( Procedures.class ).getAllProcedures().stream()
                .sorted( ( a, b ) -> a.name().toString().compareTo( b.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @Description( "List all user functions in the DBMS." )
    @Procedure(name = "dbms.functions", mode = DBMS)
    public Stream<FunctionResult> listFunctions()
    {
        return graph.getDependencyResolver().resolveDependency( Procedures.class ).getAllFunctions().stream()
                .sorted( ( a, b ) -> a.name().toString().compareTo( b.name().toString() ) )
                .map( FunctionResult::new );
    }

    public static class FunctionResult
    {
        public final String name;
        public final String signature;
        public final String description;

        private FunctionResult( UserFunctionSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
        }
    }

    public static class ProcedureResult
    {
        public final String name;
        public final String signature;
        public final String description;

        private ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
        }
    }
}
