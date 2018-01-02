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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Collection;

import static org.neo4j.helpers.collection.MapUtil.map;

public final class NodeRepresentation extends ObjectRepresentation implements ExtensibleRepresentation,
        EntityRepresentation
{
    private final Node node;

    public NodeRepresentation( Node node )
    {
        super( RepresentationType.NODE );
        this.node = node;
    }

    @Override
    public String getIdentity()
    {
        return Long.toString( node.getId() );
    }

    @Override
    @Mapping( "self" )
    public ValueRepresentation selfUri()
    {
        return ValueRepresentation.uri( path( "" ) );
    }

    public long getId()
    {
        return node.getId();
    }

    private String path( String path )
    {
        return "node/" + node.getId() + path;
    }

    static String path( Node node )
    {
        return "node/" + node.getId();
    }

    @Mapping( "create_relationship" )
    public ValueRepresentation relationshipCreationUri()
    {
        return ValueRepresentation.uri( path( "/relationships" ) );
    }

    @Mapping( "all_relationships" )
    public ValueRepresentation allRelationshipsUri()
    {
        return ValueRepresentation.uri( path( "/relationships/all" ) );
    }

    @Mapping( "incoming_relationships" )
    public ValueRepresentation incomingRelationshipsUri()
    {
        return ValueRepresentation.uri( path( "/relationships/in" ) );
    }

    @Mapping( "outgoing_relationships" )
    public ValueRepresentation outgoingRelationshipsUri()
    {
        return ValueRepresentation.uri( path( "/relationships/out" ) );
    }

    @Mapping( "all_typed_relationships" )
    public ValueRepresentation allTypedRelationshipsUriTemplate()
    {
        return ValueRepresentation.template( path( "/relationships/all/{-list|&|types}" ) );
    }

    @Mapping( "incoming_typed_relationships" )
    public ValueRepresentation incomingTypedRelationshipsUriTemplate()
    {
        return ValueRepresentation.template( path( "/relationships/in/{-list|&|types}" ) );
    }

    @Mapping( "outgoing_typed_relationships" )
    public ValueRepresentation outgoingTypedRelationshipsUriTemplate()
    {
        return ValueRepresentation.template( path( "/relationships/out/{-list|&|types}" ) );
    }
    
    @Mapping( "labels" )
    public ValueRepresentation labelsUriTemplate()
    {
        return ValueRepresentation.template( path( "/labels" ) );
    }

    @Mapping( "properties" )
    public ValueRepresentation propertiesUri()
    {
        return ValueRepresentation.uri( path( "/properties" ) );
    }

    @Mapping( "property" )
    public ValueRepresentation propertyUriTemplate()
    {
        return ValueRepresentation.template( path( "/properties/{key}" ) );
    }

    @Mapping( "traverse" )
    public ValueRepresentation traverseUriTemplate()
    {
        return ValueRepresentation.template( path( "/traverse/{returnType}" ) );
    }

    @Mapping( "paged_traverse" )
    public ValueRepresentation pagedTraverseUriTemplate()
    {
        return ValueRepresentation.template( path( "/paged/traverse/{returnType}{?pageSize,leaseTime}" ) );
    }

    @Mapping( "metadata" )
    public MapRepresentation metadata()
    {
        Collection<String> labels = IteratorUtil.asCollection( new IterableWrapper<String, Label>( node.getLabels() )
        {
            @Override
            protected String underlyingObjectToObject( Label label )
            {
                return label.name();
            }
        });
        return new MapRepresentation( map( "id", node.getId(), "labels" , labels ) );
    }

    @Override
    void extraData( MappingSerializer serializer )
    {
        MappingWriter writer = serializer.writer;
        MappingWriter properties = writer.newMapping( RepresentationType.PROPERTIES, "data" );
        new PropertiesRepresentation( node ).serialize( properties );
        if ( writer.isInteractive() )
        {
            serializer.putList( "relationship_types", ListRepresentation.relationshipTypes(
                    GlobalGraphOperations.at( node.getGraphDatabase() ).getAllRelationshipTypes() ) );
        }
        properties.done();
    }

    public static ListRepresentation list( Iterable<Node> nodes )
    {
        return new ListRepresentation( RepresentationType.NODE, new IterableWrapper<Representation, Node>( nodes )
        {
            @Override
            protected Representation underlyingObjectToObject( Node node )
            {
                return new NodeRepresentation( node );
            }
        } );
    }
}
