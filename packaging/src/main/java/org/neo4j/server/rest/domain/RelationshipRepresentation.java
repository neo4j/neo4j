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

import org.neo4j.graphdb.Relationship;

public class RelationshipRepresentation implements Representation {

    private final long id;
    private final URI baseUri;
    private final PropertiesMap properties;
    private final long startNodeId;
    private final long endNodeId;
    private final String type;

    public RelationshipRepresentation(URI baseUri, Relationship relationship) {
        this.baseUri = baseUri;
        this.id = relationship.getId();
        this.startNodeId = relationship.getStartNode().getId();
        this.endNodeId = relationship.getEndNode().getId();
        this.properties = new PropertiesMap(relationship);
        this.type = relationship.getType().name();
    }

    public URI selfUri() {
        return uri(link(""));
    }

    private URI uri(String link) {
        try {
            return new URI(link);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
    private String link( String path )
    {
        return link( baseUri, getId(), path );
    }

    static String link(URI baseUri, long id, String path) {
        return baseUri + "relationship/" + id + path;
    }

    private String nodeLink(long nodeId) {
        return NodeRepresentation.link(baseUri, nodeId, "");
    }

    public long getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public URI startNodeUri() {
        return uri(nodeLink(startNodeId));
    }

    public URI endNodeUri() {
        return uri(nodeLink(endNodeId));
    }

    public URI propertiesUri() {
        return uri(link("/properties"));
    }

    public String propertyUriTemplate() {
        return link("/properties/{key}");
    }

    public PropertiesMap getProperties() {
        return properties;
    }

    public Map<String, Object> serialize() {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("self", selfUri().toString());
        result.put("start", startNodeUri().toString());
        result.put("end", endNodeUri().toString());
        result.put("type", getType());
        result.put("properties", propertiesUri().toString());
        result.put("property", propertyUriTemplate());
        result.put("data", properties.serialize());
        return result;
    }
}
