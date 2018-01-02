/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import java.io.IOException;

import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcherBuilder;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.logging.AssertableLogProvider.inLog;

public class CypherLoggingTest
{
    @Test
    public void shouldNotLogQueries() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ExecutionEngine engine = engineWithLogger( logProvider );

        // when
        engine.execute( "CREATE (n:Reference) CREATE (foo {test:'me'}) RETURN n" );
        engine.execute( "MATCH n RETURN n" );

        // then
        inLog( org.neo4j.cypher.ExecutionEngine.class );
        logProvider.assertNoLoggingOccurred();
    }

    private ExecutionEngine engineWithLogger( LogProvider logProvider ) throws IOException
    {
        return new ExecutionEngine( new TestGraphDatabaseFactory().newImpermanentDatabase(), logProvider );
    }
}
