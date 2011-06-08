/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.SyntaxError;
import org.neo4j.cypher.commands.Query;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.junit.matchers.JUnitMatchers.hasItem;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
// START SNIPPET: JavaQuery
public class JavaExecutionEngineTests
{
    private CypherParser parser;
    private ImpermanentGraphDatabase db;
    private ExecutionEngine engine;

    @Before
    public void setUp() throws Exception
    {
        parser = new CypherParser();
        db = new ImpermanentGraphDatabase();
        engine = new ExecutionEngine( db );
    }

    @Test
    public void exampleQuery() throws Exception
    {
        GraphDatabaseService db = new ImpermanentGraphDatabase();
        CypherParser parser = new CypherParser();
        ExecutionEngine engine = new ExecutionEngine( db );
        Query query = parser.parse( "start n=(0) return n" );
        ExecutionResult result = engine.execute( query );

        assertThat( result.columns(), hasItem( "n" ) );
        Iterator<Node> n_column = result.columnAs( "n" );
        assertThat( asIterable( n_column ), hasItem( db.getNodeById( 0 ) ) );
        assertThat( result.toString(), containsString("Node[0]") );
    }

    @Test
    public void runSimpleQuery() throws Exception
    {
        Collection<Node> nodes = testQuery( "start n=(0) return n" );
        assertThat( nodes, hasItem( db.getReferenceNode() ) );
    }

    private Collection<Node> testQuery( String query ) throws SyntaxError
    {
        Query compiledQuery = parser.parse( query );
        ExecutionResult result = engine.execute( compiledQuery );
        return IteratorUtil.asCollection( asIterable( result.<Node>columnAs( "n" ) ) );
    }
}
// END SNIPPET: JavaQuery
