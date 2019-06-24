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
package org.neo4j.procedure.builtin;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.query.FunctionInformation;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

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

    @Admin
    @Description( "List the currently active config of Neo4j." )
    @Procedure( name = "dbms.listConfig", mode = DBMS )
    public Stream<ConfigResult> listConfig( @Name( value = "searchString", defaultValue = "" ) String searchString )
    {
        String lowerCasedSearchString = searchString.toLowerCase();
        List<ConfigResult> results = new ArrayList<>();

        Config config = graph.getDependencyResolver().resolveDependency( Config.class );

        config.getValues().forEach( ( setting, value ) -> {
            if ( !setting.internal() && setting.name().toLowerCase().contains( lowerCasedSearchString ) )
            {
                results.add( new ConfigResult( setting, value ) );
            }
        } );
        return results.stream().sorted( Comparator.comparing( c -> c.name ) );
    }

    @Description( "List all procedures in the DBMS." )
    @Procedure( name = "dbms.procedures", mode = DBMS )
    public Stream<ProcedureResult> listProcedures()
    {
        securityContext.assertCredentialsNotExpired();
        return graph.getDependencyResolver().resolveDependency( GlobalProceduresRegistry.class ).getAllProcedures().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @Description( "List all functions in the DBMS." )
    @Procedure( name = "dbms.functions", mode = DBMS )
    public Stream<FunctionResult> listFunctions()
    {
        securityContext.assertCredentialsNotExpired();

        DependencyResolver resolver = graph.getDependencyResolver();
        QueryExecutionEngine queryExecutionEngine = resolver.resolveDependency( QueryExecutionEngine.class );
        List<FunctionInformation> providedLanguageFunctions = queryExecutionEngine.getProvidedLanguageFunctions();

        // gets you all functions provided by the query language
        Stream<FunctionResult> languageFunctions =
                providedLanguageFunctions.stream().map( FunctionResult::new );

        // gets you all non-aggregating functions that are registered in the db (incl. those from libs like apoc)
        Stream<FunctionResult> loadedFunctions = resolver.resolveDependency( GlobalProceduresRegistry.class ).getAllNonAggregatingFunctions()
                .map( f -> new FunctionResult( f, false ) );

        // gets you all aggregation functions that are registered in the db (incl. those from libs like apoc)
        Stream<FunctionResult> loadedAggregationFunctions = resolver.resolveDependency( GlobalProceduresRegistry.class ).getAllAggregatingFunctions()
                .map( f -> new FunctionResult( f, true ) );

        return Stream.concat( Stream.concat( languageFunctions, loadedFunctions ), loadedAggregationFunctions )
                .sorted( Comparator.comparing( a -> a.name ) );
    }

    @Admin
    @Description( "Clears all query caches." )
    @Procedure( name = "dbms.clearQueryCaches", mode = DBMS )
    public Stream<StringResult> clearAllQueryCaches()
    {
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
        public final boolean aggregating;

        private FunctionResult( UserFunctionSignature signature, boolean isAggregation )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            this.aggregating = isAggregation;
        }

        private FunctionResult( FunctionInformation info )
        {
            this.name = info.getFunctionName();
            this.signature = info.getSignature();
            this.description = info.getDescription();
            this.aggregating = info.isAggregationFunction();
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
