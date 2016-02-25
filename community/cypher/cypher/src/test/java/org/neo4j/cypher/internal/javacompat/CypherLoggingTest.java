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
package org.neo4j.cypher.internal.javacompat;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.logging.AssertableLogProvider.inLog;

public class CypherLoggingTest
{
    private static final Map<String,Object> NO_PARAMS = Collections.emptyMap();
    private static final QuerySession SESSION = QueryEngineProvider.embeddedSession();

    @Test
    public void shouldNotLogQueries() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        GraphDatabaseCypherService database =
                new GraphDatabaseCypherService( new TestGraphDatabaseFactory().newImpermanentDatabase() );
        ExecutionEngine engine = new ExecutionEngine( database, logProvider );

        // when
        engine.executeQuery( "CREATE (n:Reference) CREATE (foo {test:'me'}) RETURN n", NO_PARAMS, SESSION );
        engine.executeQuery( "MATCH (n) RETURN n", NO_PARAMS, SESSION );

        // then
        inLog( org.neo4j.cypher.internal.ExecutionEngine.class );
        logProvider.assertNoLoggingOccurred();
    }
}
