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
package org.neo4j.kernel.impl.traversal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.test.GraphDefinition;
import org.neo4j.test.GraphDescription;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.*;

public abstract class TraversalTestBase extends AbstractNeo4jTestCase
{
    private Map<String, Node> nodes;

    @Override
    protected boolean restartGraphDbBetweenTests()
    {
        return true;
    }

    protected Node node( String name )
    {
        return nodes.get( name );
    }

    protected Node getNode( long id )
    {
        return getGraphDb().getNodeById( id );
    }

    protected Transaction beginTx()
    {
        return getGraphDb().beginTx();
    }

    protected void createGraph( String... description )
    {
        nodes = createGraph( GraphDescription.create( description ) );
    }

    private Map<String, Node> createGraph( GraphDefinition graph )
    {
        try ( Transaction tx = beginTx() )
        {
            Map<String, Node> result = graph.create( getGraphDb() );
            tx.success();
            return result;
        }
    }

    protected Node getNodeWithName( String name )
    {
        for ( Node node : GlobalGraphOperations.at( getGraphDb() ).getAllNodes() )
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
                    + ") at level " + (3 - levels.size()),
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

    protected <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, String... expected )
    {
        expect( items, representation, new HashSet<String>(
                Arrays.asList( expected ) ) );
    }

    protected <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, Set<String> expected )
    {
        Collection<String> encounteredItems = new ArrayList<String>();
        try (Transaction tx = beginTx())
        {
            for ( T item : items )
            {
                String repr = representation.represent( item );
                assertTrue( repr + " not expected ", expected.remove( repr ) );
                encounteredItems.add( repr );
            }
            tx.success();
        }

        if ( !expected.isEmpty() )
        {
            fail( "The exepected elements " + expected + " were not returned. Returned were: " + encounteredItems );
        }
    }

    protected void expectNodes( Traverser traverser, String... nodes )
    {
        expect( traverser.nodes(), NAME_PROPERTY_REPRESENTATION, nodes );
    }

    protected void expectRelationships( Traverser traverser,
            String... relationships )
    {
        expect( traverser.relationships(), new RelationshipRepresentation(
                NAME_PROPERTY_REPRESENTATION ), relationships );
    }

    protected void expectPaths( Traverser traverser, String... paths )
    {
        expectPaths( traverser, new HashSet<String>( Arrays.asList( paths ) ) );
    }

    protected void expectPaths( Traverser traverser, Set<String> expected )
    {
        expect( traverser, new NodePathRepresentation(
                NAME_PROPERTY_REPRESENTATION ), expected );
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
        StringBuilder buffer = new StringBuilder();
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
}
