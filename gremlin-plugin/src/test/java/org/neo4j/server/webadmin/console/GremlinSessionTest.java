/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.webadmin.console;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappingDatabase;
import org.neo4j.test.ImpermanentGraphDatabase;

public class GremlinSessionTest
{
    private static final String NEWLINE = System.getProperty( "line.separator" );
    private static ScriptSession session;
    private static Database database;

    @Test
    public void retrievesTheReferenceNode()
    {
        String result = session.evaluate( "g" ).first();

        assertEquals( String.format( "neo4jgraph[%s]" + NEWLINE, database.getGraph().toString() ), result );
    }

    @Test
    public void multiLineTest()
    {
        String result = session.evaluate( "for (i in 0..2) {" ).first();
        result = session.evaluate( "println 'hi'" ).first();
        result = session.evaluate( "}" ).first();
        result = session.evaluate( "i = 2" ).first();

        assertEquals( "2" + NEWLINE, result );
    }

    @Test
    public void testGremlinVersion()
    {
        String result = session.evaluate( "Gremlin.version()" ).first();
        assertEquals( "1.5" + NEWLINE, result );
    }

    @Test
    public void canCreateNodesAndEdgesInGremlinLand()
    {
        String result = session.evaluate( "g.addVertex(null)" ).first();
        assertEquals( "v[1]" + NEWLINE, result );
        result = session.evaluate( "g.V.next(2)" ).first();
        assertEquals( "v[0]" + NEWLINE+"v[1]" + NEWLINE, result );
        result = session.evaluate( "g.addVertex(null)" ).first();
        assertEquals( "v[2]" + NEWLINE, result );
        result = session.evaluate( "g.addEdge(g.v(1), g.v(2), 'knows')" ).first();
        assertEquals( "e[0][1-knows->2]" + NEWLINE, result );
        result = session.evaluate( "g.v(1).out" ).first();
        assertEquals( "v[2]" + NEWLINE, result );
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        database = new WrappingDatabase( new ImpermanentGraphDatabase() );
        session = new GremlinSession( database );
    }

    @AfterClass
    public static void shutdownDatabase()
    {
        database.getGraph().shutdown();
    }
}
