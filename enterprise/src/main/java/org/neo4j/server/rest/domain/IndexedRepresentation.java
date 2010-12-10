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

/**
 * Bad name. It's a representation of what to return when a Node/Relationship has been
 * indexed... i.e. the index URI to be used if unindexing it later.
 */
public class IndexedRepresentation implements Representation
{
    private final URI baseUri;
    private final String key;
    private final Object value;
    private final long objectId;
    private final String indexName;
    private String indexedElementType;
    public static final String NODE = "node"; // TODO: probably belongs somewhere else, or should be refactored into obviation
    public static final String RELATIONSHIP = "relationship";

    public IndexedRepresentation( URI baseUri, String indexedElementType, String indexName, String key, Object value,
                                  long objectId )
    {
        this.baseUri = baseUri;
        this.indexName = indexName;
        this.key = URIHelper.encode( key );
        this.value = URIHelper.encode( value.toString() );
        this.objectId = objectId;
        this.indexedElementType = indexedElementType;
    }

    public Object serialize()
    {
        return baseUri.toString() + "index/" + indexedElementType + "/" + indexName + "/" + key + "/" +
                value + "/" + objectId;
    }
}
