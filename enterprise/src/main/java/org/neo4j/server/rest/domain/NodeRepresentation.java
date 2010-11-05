/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.rest.domain;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;

public class NodeRepresentation implements Representation
{
    private final long id;
    private final URI baseUri;
    private final PropertiesMap properties;

    public NodeRepresentation( URI baseUri, Node node )
    {
        this.baseUri = baseUri;
        this.id = node.getId();
        this.properties = new PropertiesMap( node );
    }

    public URI selfUri()
    {
        return uri( "" );
    }

    private URI uri( String path )
    {
        try
        {
            return new URI( link( path ) );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    private String link( String path )
    {
        return link( baseUri, getId(), path );
    }

    static String link( URI baseUri, long id, String path )
    {
        return baseUri.toString() + "node/" + id + path;
    }

    public Map<String, Object> serialize()
    {
        HashMap<String, Object> result = new HashMap<String, Object>();
        result.put( "self", selfUri().toString() );
        result.put( "create_relationship", relationshipCreationUri().toString() );
        result.put( "all_relationships", allRelationshipsUri().toString() );
        result.put( "incoming_relationships",
                incomingRelationshipsUri().toString() );
        result.put( "outgoing_relationships",
                outgoingRelationshipsUri().toString() );
        result.put( "all_typed_relationships",
                allTypedRelationshipsUriTemplate() );
        result.put( "incoming_typed_relationships",
                incomingTypedRelationshipsUriTemplate() );
        result.put( "outgoing_typed_relationships",
                outgoingTypedRelationshipsUriTemplate() );
        result.put( "properties", propertiesUri().toString() );
        result.put( "property", propertyUriTemplate() );
        result.put( "traverse", traverseUriTemplate() );
        result.put( "data", properties.serialize() );
        return result;
    }

    public long getId()
    {
        return id;
    }

    public PropertiesMap getProperties()
    {
        return properties;
    }

    public URI allRelationshipsUri()
    {
        return uri( "/relationships/all" );
    }

    public URI incomingRelationshipsUri()
    {
        return uri( "/relationships/in" );
    }

    public URI outgoingRelationshipsUri()
    {
        return uri( "/relationships/out" );
    }

    public String allTypedRelationshipsUriTemplate()
    {
        return link( "/relationships/all/{-list|&|types}" );
    }

    public String incomingTypedRelationshipsUriTemplate()
    {
        return link( "/relationships/in/{-list|&|types}" );
    }

    public String outgoingTypedRelationshipsUriTemplate()
    {
        return link( "/relationships/out/{-list|&|types}" );
    }

    public URI relationshipCreationUri()
    {
        return uri( "/relationships" );
    }

    public URI propertiesUri()
    {
        return uri( "/properties" );
    }

    public String propertyUriTemplate()
    {
        return link( "/properties/{key}" );
    }

    public String traverseUriTemplate()
    {
        return link( "/traverse/{returnType}" );
    }
}
