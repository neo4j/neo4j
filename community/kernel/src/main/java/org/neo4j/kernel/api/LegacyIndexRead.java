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
package org.neo4j.kernel.api;

import java.util.Map;

import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;

public interface LegacyIndexRead
{
    Map<String, String> nodeLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException;

    Map<String, String> relationshipLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits nodeLegacyIndexGet( String indexName, String key, Object value )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits nodeLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException;

    LegacyIndexHits nodeLegacyIndexQuery( String indexName, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    LegacyIndexHits relationshipLegacyIndexGet( String name, String key, Object valueOrNull, long startNode,
            long endNode ) throws LegacyIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    LegacyIndexHits relationshipLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject,
            long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    /**
     * @param startNode -1 if ignored.
     * @param endNode -1 if ignored.
     */
    LegacyIndexHits relationshipLegacyIndexQuery( String indexName, Object queryOrQueryObject,
            long startNode, long endNode )
            throws LegacyIndexNotFoundKernelException;

    String[] nodeLegacyIndexesGetAll();

    String[] relationshipLegacyIndexesGetAll();
}
