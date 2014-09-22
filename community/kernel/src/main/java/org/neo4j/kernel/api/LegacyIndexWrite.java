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

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;

import java.util.Map;

public interface LegacyIndexWrite
{
    /**
     * This access the index if it exists, and creates one in a separate
     * transaction if not yet available.
     *
     * @param indexName
     * @param customConfig
     */
    void nodeLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig );

    /**
     * This is used by the above to explicitly create the index if necessary.
     *
     * @param indexName
     * @param customConfig
     */
    void nodeLegacyIndexCreate( String indexName, Map<String, String> customConfig );

    /**
     * This access the index if it exists, and creates one in a separate
     * transaction if not yet available.
     *
     * @param indexName
     * @param customConfig
     */
    void relationshipLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig );

    /**
     * This is used by the above to explicitly create the index if necessary.
     *
     * @param indexName
     * @param customConfig
     */
    void relationshipLegacyIndexCreate( String indexName, Map<String, String> customConfig );

    String nodeLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException;

    String relationshipLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException;

    String nodeLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException;

    String relationshipLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException;

    void nodeAddToLegacyIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException;

    void nodeRemoveFromLegacyIndex( String indexName, long node, String key, Object value )
            throws LegacyIndexNotFoundKernelException;

    void nodeRemoveFromLegacyIndex( String indexName, long node, String key ) throws LegacyIndexNotFoundKernelException;

    void nodeRemoveFromLegacyIndex( String indexName, long node ) throws LegacyIndexNotFoundKernelException;

    void relationshipAddToLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException;

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key, Object value )
            throws LegacyIndexNotFoundKernelException;

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key )
            throws LegacyIndexNotFoundKernelException;

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship ) throws LegacyIndexNotFoundKernelException;

    void nodeLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException;

    void relationshipLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException;
}
