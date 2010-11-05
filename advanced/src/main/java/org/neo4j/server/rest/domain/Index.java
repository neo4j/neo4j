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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

/**
 * An abstraction of an {@link IndexService} which can handle either
 * {@link Node}s or {@link Relationship}s.
 * 
 * This will probably be removed if/when {@link IndexService} supports
 * indexing of relationships.
 */
public interface Index<T extends PropertyContainer>
{
    boolean add( T object, String key, Object value );
    
    boolean remove( T object, String key, Object value );
    
    boolean contains( T object, String key, Object value );
    
    IndexHits<T> get( String key, Object value );
}
