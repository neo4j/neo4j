/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.operations;

import java.util.Map;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.KernelStatement;

public interface ExplicitIndexWriteOperations
{
    void nodeExplicitIndexCreateLazily( KernelStatement statement, String indexName, Map<String, String> customConfig );

    void nodeExplicitIndexCreate( KernelStatement statement, String indexName, Map<String, String> customConfig );

    void relationshipExplicitIndexCreateLazily( KernelStatement statement, String indexName,
            Map<String, String> customConfig );

    void relationshipExplicitIndexCreate( KernelStatement statement, String indexName,
            Map<String, String> customConfig );

    String nodeExplicitIndexSetConfiguration( KernelStatement statement, String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException;

    String relationshipExplicitIndexSetConfiguration( KernelStatement statement, String indexName, String key,
            String value ) throws ExplicitIndexNotFoundKernelException;

    String nodeExplicitIndexRemoveConfiguration( KernelStatement statement, String indexName, String key )
            throws ExplicitIndexNotFoundKernelException;

    String relationshipExplicitIndexRemoveConfiguration( KernelStatement statement, String indexName, String key )
            throws ExplicitIndexNotFoundKernelException;

    void nodeAddToExplicitIndex( KernelStatement statement, String indexName, long node, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException;

    void nodeRemoveFromExplicitIndex( KernelStatement statement, String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException;

    void nodeRemoveFromExplicitIndex( KernelStatement statement, String indexName, long node, String key )
            throws ExplicitIndexNotFoundKernelException;

    void nodeRemoveFromExplicitIndex( KernelStatement statement, String indexName, long node )
            throws ExplicitIndexNotFoundKernelException;

    void relationshipAddToExplicitIndex( KernelStatement statement, String indexName, long relationship, String key,
            Object value ) throws EntityNotFoundException, ExplicitIndexNotFoundKernelException;

    void relationshipRemoveFromExplicitIndex( KernelStatement statement, String indexName, long relationship, String key,
            Object value ) throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

    void relationshipRemoveFromExplicitIndex( KernelStatement statement, String indexName, long relationship, String key )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

    void relationshipRemoveFromExplicitIndex( KernelStatement statement, String indexName, long relationship )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

    void nodeExplicitIndexDrop( KernelStatement statement, String indexName ) throws ExplicitIndexNotFoundKernelException;

    void relationshipExplicitIndexDrop( KernelStatement statement, String indexName ) throws
            ExplicitIndexNotFoundKernelException;
}
