/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;

import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.procedure.Mode.DBMS;

public class QueryEngineProcedures
{
    @Context
    public GraphDatabaseAPI graph;

    @Context
    public SecurityContext context;

    @Description( "Clears all query caches." )
    @Procedure( name = "dbms.clearQueryCaches", mode = DBMS )
    public Stream<StringResult> clearAllQueryCaches()
    {
        if ( !context.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }

        QueryExecutionEngine queryExecutionEngine = graph.getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        long numberOfClearedQueries = queryExecutionEngine.clearQueryCaches() - 1; // this query itself does not count

        String result = numberOfClearedQueries == 0 ? "Query cache already empty."
                                                    : "Query caches successfully cleared of " + numberOfClearedQueries + " queries.";
        return Stream.of( new StringResult( result ) );
    }

    public class StringResult
    {
        public final String value;

        StringResult( String value )
        {
            this.value = value;
        }
    }
}
