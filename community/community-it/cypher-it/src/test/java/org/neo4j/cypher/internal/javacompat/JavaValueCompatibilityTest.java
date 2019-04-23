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
package org.neo4j.cypher.internal.javacompat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class JavaValueCompatibilityTest
{
    private GraphDatabaseService  db;
    private DatabaseManagementService managementService;

    @Before
    public void setUp()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @After
    public void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    public void collections_in_collections_look_alright()
    {
        Result result = db.execute( "CREATE (n:TheNode) RETURN [[ [1,2],[3,4] ],[[5,6]]] as x" );
        Map<String, Object> next = result.next();
        @SuppressWarnings( "unchecked" ) //We know it's a collection.
        List<List<Object>> x = (List<List<Object>>)next.get( "x" );
        Iterable objects = x.get( 0 );

        assertThat(objects, isA(Iterable.class));
    }
}
