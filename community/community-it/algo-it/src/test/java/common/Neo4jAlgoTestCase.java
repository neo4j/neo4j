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
package common;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base class for test cases working on a NeoService. It sets up a NeoService
 * and a transaction.
 * @author Patrik Larsson
 */
public abstract class Neo4jAlgoTestCase
{
    protected static GraphDatabaseService graphDb;
    protected static SimpleGraphBuilder graph;
    protected Transaction tx;

    public enum MyRelTypes implements RelationshipType
    {
        R1, R2, R3
    }

    @BeforeClass
    public static void setUpGraphDb()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        graph = new SimpleGraphBuilder( graphDb, MyRelTypes.R1 );
    }

    @Before
    public void setUpTransaction()
    {
        tx = graphDb.beginTx();
    }

    @AfterClass
    public static void tearDownGraphDb()
    {
        graphDb.shutdown();
    }

    @After
    public void tearDownTransactionAndGraph()
    {
        graph.clear();
        tx.success();
        tx.close();
    }

    protected void assertPathDef( Path path, String... names )
    {
        int i = 0;
        for ( Node node : path.nodes() )
        {
            assertEquals( "Wrong node " + i + " in " + getPathDef( path ),
                    names[i++], node.getProperty( SimpleGraphBuilder.KEY_ID ) );
        }
        assertEquals( names.length, i );
    }

    protected void assertPath( Path path, String commaSeparatedNodePath )
    {
        String[] nodeIds = commaSeparatedNodePath.split( "," );
        Node[] nodes = new Node[nodeIds.length];
        int i = 0;
        for ( String id : nodeIds )
        {
            nodes[i] = graph.getNode( id );
            i++;
        }
        assertPath( path, nodes );
    }

    protected void assertPath( Path path, Node... nodes )
    {
        int i = 0;
        for ( Node node : path.nodes() )
        {
            assertEquals( "Wrong node " + i + " in " + getPathDef( path ),
                    nodes[i++].getProperty( SimpleGraphBuilder.KEY_ID ), node.getProperty( SimpleGraphBuilder.KEY_ID ) );
        }
        assertEquals( nodes.length, i );
    }

    protected <E> void assertContains( Iterable<E> actual, E... expected )
    {
        Set<E> expectation = new HashSet<>( Arrays.asList( expected ) );
        for ( E element : actual )
        {
            if ( !expectation.remove( element ) )
            {
                fail( "unexpected element <" + element + ">" );
            }
        }
        if ( !expectation.isEmpty() )
        {
            fail( "the expected elements <" + expectation
                  + "> were not contained" );
        }
    }

    public String getPathDef( Path path )
    {
        StringBuilder builder = new StringBuilder();
        for ( Node node : path.nodes() )
        {
            if ( builder.length() > 0 )
            {
                builder.append( "," );
            }
            builder.append( node.getProperty( SimpleGraphBuilder.KEY_ID ) );
        }
        return builder.toString();
    }

    public void assertPaths( Iterable<? extends Path> paths, List<String> pathDefs )
    {
        List<String> unexpectedDefs = new ArrayList<>();
        try ( ResourceIterator<? extends Path> iterator = Iterators.asResourceIterator( paths.iterator() ) )
        {
            while ( iterator.hasNext() )
            {
                Path path = iterator.next();

                String pathDef = getPathDef( path );
                int index = pathDefs.indexOf( pathDef );
                if ( index != -1 )
                {
                    pathDefs.remove( index );
                }
                else
                {
                    unexpectedDefs.add( getPathDef( path ) );
                }
            }
        }
        assertTrue( "These unexpected paths were found: " + unexpectedDefs +
                ". In addition these expected paths weren't found:" + pathDefs, unexpectedDefs.isEmpty() );
        assertTrue( "These were expected, but not found: " + pathDefs.toString(), pathDefs.isEmpty() );
    }

    public void assertPaths( Iterable<? extends Path> paths, String... pathDefinitions )
    {
        assertPaths( paths, new ArrayList<>( Arrays.asList( pathDefinitions ) ) );
    }

    public void assertPathsWithPaths( Iterable<? extends Path> actualPaths, Path... expectedPaths )
    {
        List<String> pathDefs = new ArrayList<>( );
        for ( Path path : expectedPaths )
        {
            pathDefs.add( getPathDef( path ) );
        }
        assertPaths( actualPaths, pathDefs );
    }

    public void assertPathDef( Path expected, Path actual )
    {
        int expectedLength = expected.length();
        int actualLength = actual.length();
        assertEquals( "Actual path length " + actualLength + " differ from expected path length " + expectedLength,
                expectedLength, actualLength );
        Iterator<Node> expectedNodes = expected.nodes().iterator();
        Iterator<Node> actualNodes = actual.nodes().iterator();
        int position = 0;
        while ( expectedNodes.hasNext() && actualNodes.hasNext() )
        {
            assertEquals( "Path differ on position " + position +
                          ". Expected " + getPathDef( expected ) +
                          ", actual " + getPathDef( actual ),
                    expectedNodes.next().getProperty( SimpleGraphBuilder.KEY_ID ),
                    actualNodes.next().getProperty( SimpleGraphBuilder.KEY_ID ) );
            position++;
        }
    }
}
