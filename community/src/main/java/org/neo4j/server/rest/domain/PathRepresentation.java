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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class PathRepresentation implements Representation
{
    private final URI baseUri;
    private final Path path;

    public PathRepresentation( URI baseUri, Path path )
    {
        this.baseUri = baseUri;
        this.path = path;
    }

    public Map<String, Object> serialize()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "nodes", getNodes() );
        map.put( "relationships", getRelationships() );
        map.put( "start", NodeRepresentation.link( baseUri, path.startNode().getId(), "" ) );
        map.put( "end", NodeRepresentation.link( baseUri, path.endNode().getId(), "" ) );
        map.put( "length", path.length() );
        return map;
    }
    
    private Collection<String> getNodes()
    {
        Collection<String> nodes = new ArrayList<String>();
        for ( Node node : path.nodes() )
        {
            nodes.add( NodeRepresentation.link( baseUri, node.getId(), "" ) );
        }
        return nodes;
    }

    private Collection<String> getRelationships()
    {
        Collection<String> nodes = new ArrayList<String>();
        for ( Relationship relationship : path.relationships() )
        {
            nodes.add( RelationshipRepresentation.link( baseUri, relationship.getId(), "" ) );
        }
        return nodes;
    }
}
