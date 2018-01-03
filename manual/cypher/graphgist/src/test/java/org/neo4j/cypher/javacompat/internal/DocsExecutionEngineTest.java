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
package org.neo4j.cypher.javacompat.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocsExecutionEngineTest
{
    private static GraphDatabaseService database;
    private static DocsExecutionEngine engine;

    @Before
    public void setup()
    {
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        database = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase();
        engine = new DocsExecutionEngine( database );
    }

    @After
    public void teardown()
    {
        database.shutdown();
    }

    @Test
    public void actually_works_in_rewindable_fashion()
    {
        InternalExecutionResult result = engine.profile( "CREATE (n:Person {name:'Adam'}) RETURN n" );
        String dump = result.dumpToString();
        assertThat( dump, containsString( "1 row" ) );
        assertThat( result.javaIterator().hasNext(), equalTo( true ) );
    }

    @Test
    public void should_work_in_rewindable_fashion()
    {
        InternalExecutionResult result = engine.profile( "RETURN 'foo'" );
        String dump = result.dumpToString();
        assertThat( dump, containsString( "1 row" ) );
        assertThat( result.javaIterator().hasNext(), equalTo( true ) );
    }
}
