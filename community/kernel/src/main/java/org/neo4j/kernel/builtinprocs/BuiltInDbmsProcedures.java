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
package org.neo4j.kernel.builtinprocs;

import java.util.Comparator;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class BuiltInDbmsProcedures
{
    @Context
    public Log log;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public SecurityContext securityContext;

    @Description( "List the currently active config of Neo4j." )
    @Procedure( name = "dbms.listConfig", mode = DBMS )
    public Stream<ConfigResult> listConfig( @Name( value = "searchString", defaultValue = "" ) String searchString )
    {
        securityContext.assertCredentialsNotExpired();
        if ( !securityContext.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        Config config = graph.getDependencyResolver().resolveDependency( Config.class );
        return config.getConfigValues().values().stream()
                .filter( c -> !c.internal() )
                .map( ConfigResult::new )
                .filter( c -> c.name.toLowerCase().contains( searchString.toLowerCase() ) )
                .sorted( Comparator.comparing( c -> c.name ) );
    }

    @Description( "List all procedures in the DBMS." )
    @Procedure( name = "dbms.procedures", mode = DBMS )
    public Stream<ProcedureResult> listProcedures()
    {
        securityContext.assertCredentialsNotExpired();
        return graph.getDependencyResolver().resolveDependency( Procedures.class ).getAllProcedures().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @Description( "List all user functions in the DBMS." )
    @Procedure( name = "dbms.functions", mode = DBMS )
    public Stream<FunctionResult> listFunctions()
    {
        securityContext.assertCredentialsNotExpired();
        return graph.getDependencyResolver().resolveDependency( Procedures.class ).getAllFunctions().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( FunctionResult::new );
    }

    @Description( "Clears all query caches." )
    @Procedure( name = "dbms.clearQueryCaches", mode = DBMS )
    public Stream<StringResult> clearAllQueryCaches()
    {
        securityContext.assertCredentialsNotExpired();
        if ( !securityContext.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }

        QueryExecutionEngine queryExecutionEngine = graph.getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        long numberOfClearedQueries = queryExecutionEngine.clearQueryCaches() - 1; // this query itself does not count

        String result = numberOfClearedQueries == 0 ? "Query cache already empty."
                                                    : "Query caches successfully cleared of " + numberOfClearedQueries + " queries.";
        log.info( "Called dbms.clearQueryCaches(): " + result );
        return Stream.of( new StringResult( result ) );
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
        public final String mode;

        private ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            this.mode = signature.mode().toString();
        }
    }

    public static class StringResult
    {
        public final String value;

        StringResult( String value )
        {
            this.value = value;
        }
    }
}
