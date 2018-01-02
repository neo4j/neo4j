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
package org.neo4j.test.mocking;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.Property;

import static java.util.Arrays.asList;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.Iterables.asResourceIterable;
import static org.neo4j.helpers.collection.Iterables.reverse;
import static org.neo4j.test.mocking.Properties.properties;

public class GraphMock
{
    public static Label[] labels( String... names )
    {
        Label[] labels = new Label[names.length];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = DynamicLabel.label( names[i] );
        }
        return labels;
    }

    public static Node node( long id, Label[] labels, Property... properties )
    {
        return mockNode( id, labels, properties( properties ) );
    }

    public static Node node( long id, Properties properties, String... labels )
    {
        return mockNode( id, labels( labels ), properties );
    }

    public static Relationship relationship( long id, Properties properties, Node start, String type, Node end )
    {
        return mockRelationship( id, start, type, end, properties );
    }

    public static Relationship relationship( long id, Node start, String type, Node end, Property... properties )
    {
        return mockRelationship( id, start, type, end, properties( properties ) );
    }

    public static Link link( Relationship relationship, Node node )
    {
        return Link.link( relationship, node );
    }

    public static Path path( Node node, Link... links )
    {
        List<Node> nodes = new ArrayList<>( links.length + 1 );
        List<Relationship> relationships = new ArrayList<>( links.length );
        List<PropertyContainer> mixed = new ArrayList<>( links.length * 2 + 1 );
        nodes.add( node );
        mixed.add( node );
        Path path = mock( Path.class );
        when( path.startNode() ).thenReturn( node );
        Relationship last = null;
        for ( Link link : links )
        {
            last = link.relationship;
            relationships.add( last );
            mixed.add( last );

            node = link.checkNode( node );
            nodes.add( node );
            mixed.add( node );
        }
        when( path.endNode() ).thenReturn( node );
        when( path.iterator() ).thenAnswer( withIteratorOf( mixed ) );
        when( path.nodes() ).thenReturn( nodes );
        when( path.relationships() ).thenReturn( relationships );
        when( path.lastRelationship() ).thenReturn( last );
        when( path.length() ).thenReturn( links.length );
        when( path.reverseNodes() ).thenReturn( reverse( nodes ) );
        when( path.reverseRelationships() ).thenReturn( reverse( relationships ) );
        return path;
    }

    private static <T> Answer<Iterator<T>> withIteratorOf( final Iterable<T> iterable )
    {
        return new Answer<Iterator<T>>()
        {
            @Override
            public Iterator<T> answer( InvocationOnMock invocation ) throws Throwable
            {
                return iterable.iterator();
            }
        };
    }

    private static Node mockNode( long id, Label[] labels, Properties properties )
    {
        Node node = mockPropertyContainer( Node.class, properties );
        when( node.getId() ).thenReturn( id );
        when( node.getLabels() ).thenReturn( asResourceIterable( asList( labels ) ) );
        return node;
    }

    private static Relationship mockRelationship( long id, Node start, String type, Node end, Properties properties )
    {
        Relationship relationship = mockPropertyContainer( Relationship.class, properties );
        when( relationship.getId() ).thenReturn( id );
        when( relationship.getStartNode() ).thenReturn( start );
        when( relationship.getEndNode() ).thenReturn( end );
        when( relationship.getType() ).thenReturn( DynamicRelationshipType.withName( type ) );
        return relationship;
    }

    private static <T extends PropertyContainer> T mockPropertyContainer( Class<T> type, Properties properties )
    {
        T container = mock( type );
        when( container.getProperty( anyString() ) ).thenAnswer( properties );
        when( container.getProperty( anyString(), any() ) ).thenAnswer( properties );
        when( container.getPropertyKeys() ).thenReturn( properties );
        when( container.getAllProperties() ).thenReturn( properties.getProperties() );
        return container;
    }
}
