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

import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;

import static org.neo4j.helpers.collection.MapUtil.map;

public final class RelationshipRepresentation extends ObjectRepresentation implements ExtensibleRepresentation,
        EntityRepresentation
{
    private final Relationship rel;

    public RelationshipRepresentation( Relationship rel )
    {
        super( RepresentationType.RELATIONSHIP );
        this.rel = rel;
    }

    @Override
    public String getIdentity()
    {
        return Long.toString( rel.getId() );
    }

    public long getId()
    {
        return rel.getId();
    }

    @Mapping( "self" )
    public ValueRepresentation selfUri()
    {
        return ValueRepresentation.uri( path( "" ) );
    }

    private String path( String path )
    {
        return "relationship/" + rel.getId() + path;
    }

    static String path( Relationship rel )
    {
        return "relationship/" + rel.getId();
    }

    @Mapping( "type" )
    public ValueRepresentation getType()
    {
        return ValueRepresentation.relationshipType( rel.getType() );
    }

    @Mapping( "start" )
    public ValueRepresentation startNodeUri()
    {
        return ValueRepresentation.uri( NodeRepresentation.path( rel.getStartNode() ) );
    }

    @Mapping( "end" )
    public ValueRepresentation endNodeUri()
    {
        return ValueRepresentation.uri( NodeRepresentation.path( rel.getEndNode() ) );
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

    @Mapping( "metadata" )
    public MapRepresentation metadata()
    {
        return new MapRepresentation( map( "id", rel.getId(), "type", rel.getType().name() ) );
    }

    @Override
    void extraData( MappingSerializer serializer )
    {
        MappingWriter properties = serializer.writer.newMapping( RepresentationType.PROPERTIES, "data" );
        new PropertiesRepresentation( rel ).serialize( properties );
        properties.done();
    }

    public static ListRepresentation list( Iterable<Relationship> relationships )
    {
        return new ListRepresentation( RepresentationType.RELATIONSHIP,
                new IterableWrapper<Representation, Relationship>( relationships )
                {
                    @Override
                    protected Representation underlyingObjectToObject( Relationship relationship )
                    {
                        return new RelationshipRepresentation( relationship );
                    }
                } );
    }
}
