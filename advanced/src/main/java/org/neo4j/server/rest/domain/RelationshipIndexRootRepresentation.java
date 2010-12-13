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

import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;

public class RelationshipIndexRootRepresentation implements Representation
{
    private final URI baseUri;
    private IndexManager indexManager;

    public RelationshipIndexRootRepresentation( URI baseUri, IndexManager indexManager )
    {
        this.baseUri = baseUri;
        this.indexManager = indexManager;
    }

    public URI selfUri()
    {
        try
        {
            return new URI( baseUri.toString() + "index" );
        } catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    public Map<String, Object> serialize()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        indexManager.relationshipIndexNames();

        for ( String indexName : indexManager.relationshipIndexNames() )
        {
            RelationshipIndex index = indexManager.forRelationships( indexName );
            map.put( indexName, new NodeIndexRepresentation( baseUri, indexName, indexManager.getConfiguration( index ) ).serialize() );
        }

        return map;
    }
}
