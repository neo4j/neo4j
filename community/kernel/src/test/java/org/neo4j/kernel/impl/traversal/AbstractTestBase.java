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
package org.neo4j.kernel.impl.traversal;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.GraphDefinition;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

public abstract class AbstractTestBase
{
    private static ImpermanentGraphDatabase graphdb;
    private static Map<String, Node> nodes;

    @Before
    public final void createDb()
    {
        graphdb = new ImpermanentGraphDatabase();
    }

    @After
    public final void afterSuite()
    {
        graphdb.shutdown();
    }

    protected static final Node node( String name )
    {
        return nodes.get( name );
    }

    protected static final Node getNode( long id )
    {
        return graphdb.getNodeById( id );
    }

    protected static final Transaction beginTx()
    {
        return graphdb.beginTx();
    }

    protected static void createGraph( String... description )
    {
        nodes = createGraph( GraphDescription.create( description ) );
    }

    private static Map<String, Node> createGraph( GraphDefinition graph )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Map<String, Node> result = graph.create( graphdb );
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    protected static Node getNodeWithName( String name )
    {
        for ( Node node : GlobalGraphOperations.at( graphdb ).getAllNodes() )
        {
            String nodeName = (String) node.getProperty( "name", null );
            if ( nodeName != null && nodeName.equals( name ) )
            {
                return node;
            }
        }
        return null;
    }

    protected void assertLevels( Traverser traverser, Stack<Set<String>> levels )
    {
        Set<String> current = levels.pop();
        for ( Path position : traverser )
        {
            String nodeName = (String) position.endNode().getProperty( "name" );
            if ( current.isEmpty() )
            {
                current = levels.pop();
            }
            assertTrue( "Should not contain node (" + nodeName
                        + ") at level " + ( 3 - levels.size() ),
                    current.remove( nodeName ) );
        }

        assertTrue( "Should have no more levels", levels.isEmpty() );
        assertTrue( "Should be empty", current.isEmpty() );
    }

    protected static final Representation<PropertyContainer> NAME_PROPERTY_REPRESENTATION = new PropertyRepresentation( "name" );

    protected static final Representation<Relationship> RELATIONSHIP_TYPE_REPRESENTATION = new Representation<Relationship>()
    {
        public String represent( Relationship item )
        {
            return item.getType().name();
        }
    };

    protected interface Representation<T>
    {
        String represent( T item );
    }

    protected static final class PropertyRepresentation implements
            Representation<PropertyContainer>
    {
        public PropertyRepresentation( String key )
        {
            this.key = key;
        }

        private final String key;

        public String represent( PropertyContainer item )
        {
            return (String) item.getProperty( key );
        }
    }

    protected static final class RelationshipRepresentation implements
            Representation<Relationship>
    {
        private final Representation<? super Node> nodes;
        private final Representation<? super Relationship> rel;

        public RelationshipRepresentation( Representation<? super Node> nodes )
        {
            this( nodes, RELATIONSHIP_TYPE_REPRESENTATION );
        }

        public RelationshipRepresentation( Representation<? super Node> nodes,
                Representation<? super Relationship> rel )
        {
            this.nodes = nodes;
            this.rel = rel;
        }

        public String represent( Relationship item )
        {
            return nodes.represent( item.getStartNode() ) + " "
                   + rel.represent( item ) + " "
                   + nodes.represent( item.getEndNode() );
        }
    }

    protected static final class NodePathRepresentation implements
            Representation<Path>
    {
        private final Representation<? super Node> nodes;

        public NodePathRepresentation( Representation<? super Node> nodes )
        {
            this.nodes = nodes;

        }

        public String represent( Path item )
        {
            StringBuilder builder = new StringBuilder();
            for ( Node node : item.nodes() )
            {
                builder.append( builder.length() > 0 ? "," : "" );
                builder.append( nodes.represent( node ) );
            }
            return builder.toString();
        }
    }

    protected static <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, String... expected )
    {
        expect( items, representation, new HashSet<String>(
                Arrays.asList( expected ) ) );
    }

    protected static <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, Set<String> expected )
    {
        Transaction tx = beginTx();
        Collection<String> encounteredItems = new ArrayList<String>();
        try
        {
            for ( T item : items )
            {
                String repr = representation.represent( item );
                assertTrue( repr + " not expected ", expected.remove( repr ) );
                encounteredItems.add( repr );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        if ( !expected.isEmpty() )
        {
            fail( "The exepected elements " + expected + " were not returned. Returned were: " + encounteredItems );
        }
    }

    protected static void expectNodes( Traverser traverser, String... nodes )
    {
        expect( traverser.nodes(), NAME_PROPERTY_REPRESENTATION, nodes );
    }

    protected static void expectRelationships( Traverser traverser,
            String... relationships )
    {
        expect( traverser.relationships(), new RelationshipRepresentation(
                NAME_PROPERTY_REPRESENTATION ), relationships );
    }

    protected static void expectPaths( Traverser traverser, String... paths )
    {
        expectPaths( traverser, new HashSet<String>( Arrays.asList( paths ) ) );
    }

    protected static void expectPaths( Traverser traverser, Set<String> expected )
    {
        expect( traverser, new NodePathRepresentation(
                NAME_PROPERTY_REPRESENTATION ), expected );
    }
    
    protected static void expectPath( Path path, String pathAsString )
    {
        expect( asList( path ), new NodePathRepresentation( NAME_PROPERTY_REPRESENTATION ),
                pathAsString );
    }
    
    public static <E> void assertContains( Iterator<E> actual, E... expected )
    {
        assertContains( IteratorUtil.asIterable( actual ), expected );
    }

    public static <E> void assertContains( Iterable<E> actual, E... expected )
    {
        Set<E> expectation = new HashSet<E>( Arrays.asList( expected ) );
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

    public static <T> void assertContainsInOrder( Collection<T> collection,
            T... expectedItems )
    {
        String collectionString = join( ", ", collection.toArray() );
        assertEquals( collectionString, expectedItems.length, collection.size() );
        Iterator<T> itr = collection.iterator();
        for ( int i = 0; itr.hasNext(); i++ )
        {
            assertEquals( expectedItems[i], itr.next() );
        }
    }
    
    public static <T> void assertContainsInOrder( Iterable<T> collection,
            T... expectedItems )
    {
        assertContainsInOrder( IteratorUtil.asCollection( collection ), expectedItems );
    }

    public static <T> String join( String delimiter, T... items )
    {
        StringBuffer buffer = new StringBuffer();
        for ( T item : items )
        {
            if ( buffer.length() > 0 )
            {
                buffer.append( delimiter );
            }
            buffer.append( item.toString() );
        }
        return buffer.toString();
    }
    
    protected void setRelationshipProperty( String fromNode, String toNode, String key, Object value )
    {
        for ( Relationship relationship : getNodeWithName( fromNode ).getRelationships() )
            if ( relationship.getEndNode().equals( getNodeWithName( toNode ) ) )
            {
                relationship.setProperty( key, value );
                return;
            }
        throw new IllegalArgumentException( "No relationship found between " + fromNode + " and " + toNode );
    }
}
