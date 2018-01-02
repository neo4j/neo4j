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
package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;

import java.util.ArrayList;
import java.util.Iterator;

public class PathRepresentation<P extends Path> extends ObjectRepresentation // implements
                                                                             // ExtensibleRepresentation
{
    private final P path;

    public PathRepresentation( P path )
    {
        super( RepresentationType.PATH );
        this.path = path;
    }

    /*
    @Override
    public String getIdentity()
    {
        StringBuilder result = new StringBuilder( Long.toString( path.startNode().getId() ) );
        String sep = "+";
        for ( Relationship rel : path.relationships() )
        {
            result.append( sep ).append( Long.toString( rel.getId() ) );
            sep = "-";
        }
        result.append( "+" ).append( Long.toString( path.endNode().getId() ) );
        return result.toString();
    }
    */

    protected P getPath()
    {
        return path;
    }

    @Mapping( "start" )
    public ValueRepresentation startNode()
    {
        return ValueRepresentation.uri( NodeRepresentation.path( path.startNode() ) );
    }

    @Mapping( "end" )
    public ValueRepresentation endNode()
    {
        return ValueRepresentation.uri( NodeRepresentation.path( path.endNode() ) );
    }

    @Mapping( "length" )
    public ValueRepresentation length()
    {
        return ValueRepresentation.number( path.length() );
    }

    @Mapping( "nodes" )
    public ListRepresentation nodes()
    {
        return new ListRepresentation( RepresentationType.NODE,
                new IterableWrapper<Representation, Node>( path.nodes() )
                {
                    @Override
                    protected Representation underlyingObjectToObject( Node node )
                    {
                        return ValueRepresentation.uri( NodeRepresentation.path( node ) );
                    }
                } );
    }

    @Mapping( "relationships" )
    public ListRepresentation relationships()
    {
        return new ListRepresentation( RepresentationType.RELATIONSHIP,
                new IterableWrapper<Representation, Relationship>( path.relationships() )
                {
                    @Override
                    protected Representation underlyingObjectToObject( Relationship node )
                    {
                        return ValueRepresentation.uri( RelationshipRepresentation.path( node ) );
                    }
                } );
    }

    @Mapping( "directions" )
    public ListRepresentation directions()
    {
        ArrayList<String> directionStrings = new ArrayList<>();

        Iterator<Node> nodeIterator = path.nodes().iterator();
        Iterator<Relationship> relationshipIterator = path.relationships().iterator();

        Relationship rel;
        Node startNode;
        Node endNode = nodeIterator.next();

        while ( relationshipIterator.hasNext() ) {
            rel = relationshipIterator.next();
            startNode = endNode;
            endNode = nodeIterator.next();
            if ( rel.getStartNode().equals(startNode) && rel.getEndNode().equals(endNode) ) {
                directionStrings.add( "->" );
            } else {
                directionStrings.add( "<-" );
            }
        }

        return new ListRepresentation( RepresentationType.STRING,
                new IterableWrapper<Representation, String>( directionStrings )
                {
                    @Override
                    protected Representation underlyingObjectToObject( String directionString )
                    {
                        return ValueRepresentation.string(directionString);
                    }
                } );
    }
}
