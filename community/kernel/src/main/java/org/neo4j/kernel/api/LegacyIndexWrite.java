/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api;

import java.util.Map;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public interface LegacyIndexWrite
{
    void nodeLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig );

    void relationshipLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig );

    String nodeLegacyIndexSetConfiguration( String indexName, String key, String value );

    String relationshipLegacyIndexSetConfiguration( String indexName, String key, String value );

    String nodeLegacyIndexRemoveConfiguration( String indexName, String key );

    String relationshipLegacyIndexRemoveConfiguration( String indexName, String key );

    void nodeAddToLegacyIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException;

    void nodeRemoveFromLegacyIndex( String indexName, long node, String key, Object value );

    void nodeRemoveFromLegacyIndex( String indexName, long node, String key );

    void nodeRemoveFromLegacyIndex( String indexName, long node );

    void relationshipAddToLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException;

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key, Object value );

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key );

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship );

    void nodeLegacyIndexDrop( String indexName );

    void relationshipLegacyIndexDrop( String indexName );

    long nodeLegacyIndexPutIfAbsent( long node, String key, Object value );

    long relationshipLegacyIndexPutIfAbsent( long relationship, String key, Object value );
}
