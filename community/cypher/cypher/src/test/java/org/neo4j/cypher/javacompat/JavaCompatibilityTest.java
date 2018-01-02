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
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;

public class JavaCompatibilityTest
{
    private ExecutionEngine engine;

    @Before
    public void setUp() throws IOException
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
        engine = new ExecutionEngine( db );
    }

    @Test
    public void collections_in_collections_look_aiight() throws Exception
    {
        ExecutionResult execute = engine.execute( "CREATE (n:TheNode) RETURN [[ [1,2],[3,4] ],[[5,6]]] as x" );
        Map<String, Object> next = execute.iterator().next();
        @SuppressWarnings("unchecked") //We know it's a collection.
        List<List<Object>> x = (List<List<Object>>)next.get( "x" );
        Iterable objects = x.get( 0 );

        assertThat(objects, isA(Iterable.class));
    }
}
