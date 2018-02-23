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
package org.neo4j.cypher.internal.javacompat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.logging.AssertableLogProvider.inLog;

public class CypherLoggingTest
{

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private GraphDatabaseService database;

    @BeforeEach
    public void setUp()
    {
        database = new TestGraphDatabaseFactory().setUserLogProvider( logProvider ).newImpermanentDatabase();
    }

    @AfterEach
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void shouldNotLogQueries()
    {
        // when
        database.execute( "CREATE (n:Reference) CREATE (foo {test:'me'}) RETURN n" );
        database.execute( "MATCH (n) RETURN n" );

        // then
        inLog( org.neo4j.cypher.internal.ExecutionEngine.class );
        logProvider.assertNoLoggingOccurred();
    }
}
