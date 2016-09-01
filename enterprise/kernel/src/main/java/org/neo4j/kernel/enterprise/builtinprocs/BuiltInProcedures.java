/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.neo4j.procedure.Procedure.Mode.DBMS;

public class BuiltInProcedures
{
    @Context
    public GraphDatabaseAPI graph;

    @Context
    public KernelTransaction tx;

    @Context
    public AuthSubject authSubject;

    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException, IOException
    {
        return getKernelTransactions().executingQueries().stream().filter(
                ( query ) -> isAdmin() || authSubject.doesUsernameMatch( query.authSubjectName() ) )
                .map( this::queryStatusResult );
    }

    private KernelTransactions getKernelTransactions()
    {
        DependencyResolver resolver = graph.getDependencyResolver();
        return resolver.resolveDependency( KernelTransactions.class );
    }

    private boolean isAdmin()
    {
        EnterpriseAuthSubject enterpriseAuthSubject = (EnterpriseAuthSubject) authSubject;
        return enterpriseAuthSubject.isAdmin();
    }

    private QueryStatusResult queryStatusResult( ExecutingQuery q )
    {
        return new QueryStatusResult(
                q.queryId(),
                q.authSubjectName(),
                q.queryText(),
                q.queryParameters(),
                q.startTime()
        );
    }

    public static class QueryStatusResult
    {
        public final long queryId;

        public final String username;
        public final String query;
        public final Map<String,Object> parameters;
        public final String startTime;

        QueryStatusResult( long queryId, String username, String query, Map<String,Object> parameters, long startTime )
        {
            this.queryId = queryId;
            this.username = username;
            this.query = query;
            this.parameters = parameters;
            this.startTime = OffsetDateTime.ofInstant(
                Instant.ofEpochMilli( startTime ),
                ZoneId.systemDefault()
            ).format( ISO_OFFSET_DATE_TIME );
        }
    }
}
