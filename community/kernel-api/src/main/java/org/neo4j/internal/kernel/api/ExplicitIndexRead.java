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
package org.neo4j.internal.kernel.api;

import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.values.storable.Value;

/**
 * Operations for querying and seeking in explicit indexes.
 */
public interface ExplicitIndexRead
{
    void nodeExplicitIndexLookup( NodeExplicitIndexCursor cursor, String index, String key, Value value )
            throws ExplicitIndexNotFoundKernelException;

    void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, Object query )
            throws ExplicitIndexNotFoundKernelException;

    void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, String key, Object query )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * @param indexName name of node index to check for existence.
     * @param customConfiguration if {@code null} the configuration of existing won't be matched, otherwise it will
     * be matched and a mismatch will throw {@link IllegalArgumentException}.
     * @return whether or not node explicit index with name {@code indexName} exists.
     * @throws IllegalArgumentException on index existence with provided mismatching {@code customConfiguration}.
     */
    boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration );

    void relationshipExplicitIndexLookup(
            RelationshipExplicitIndexCursor cursor, String index, String key, Value value, long source, long target )
            throws ExplicitIndexNotFoundKernelException;

    void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor, String index, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException;

    void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor, String index, String key, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException;

    /**
     * @param indexName name of relationship index to check for existence.
     * @param customConfiguration if {@code null} the configuration of existing won't be matched, otherwise it will
     * be matched and a mismatch will throw {@link IllegalArgumentException}.
     * @return whether or not relationship explicit index with name {@code indexName} exists.
     * @throws IllegalArgumentException on index existence with provided mismatching {@code customConfiguration}.
     */
    boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration );

    String[] nodeExplicitIndexesGetAll();

    String[] relationshipExplicitIndexesGetAll();
}
