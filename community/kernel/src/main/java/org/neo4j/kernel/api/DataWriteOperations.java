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
package org.neo4j.kernel.api;

import java.util.Map;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.values.storable.Value;

public interface DataWriteOperations
{
    //===========================================
    //== DATA OPERATIONS ========================
    //===========================================

    long nodeCreate();

    void nodeDelete( long nodeId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException;

    int nodeDetachDelete( long nodeId )
            throws KernelException;

    long relationshipCreate( int relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException;

    void relationshipDelete( long relationshipId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException;

    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids are retrieved from
     * {@link org.neo4j.kernel.impl.api.operations.KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement, String)}
     * or {@link
     * org.neo4j.kernel.impl.api.operations.KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationException;

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label id are retrieved from
     * {@link org.neo4j.kernel.impl.api.operations.KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement,String)}
     * or {@link
     * org.neo4j.kernel.impl.api.operations.KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    Value nodeSetProperty( long nodeId, int propertyKeyId, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException,
                    InvalidTransactionTypeKernelException, ConstraintValidationException;

    Value relationshipSetProperty( long relationshipId, int propertyKeyId, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException;

    Value graphSetProperty( int propertyKeyId, Value value );

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Value nodeRemoveProperty( long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException;

    Value relationshipRemoveProperty( long relationshipId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException;

    Value graphRemoveProperty( int propertyKeyId );

    /**
     * Creates a explicit index in a separate transaction if not yet available.
     */
    void nodeExplicitIndexCreateLazily( String indexName, Map<String, String> customConfig );

    void nodeExplicitIndexCreate( String indexName, Map<String, String> customConfig );

    //===========================================
    //== EXPLICIT INDEX OPERATIONS ================
    //===========================================

    /**
     * Creates a explicit index in a separate transaction if not yet available.
     */
    void relationshipExplicitIndexCreateLazily( String indexName, Map<String, String> customConfig );

    void relationshipExplicitIndexCreate( String indexName, Map<String, String> customConfig );

    String nodeExplicitIndexSetConfiguration( String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException;

    String relationshipExplicitIndexSetConfiguration( String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException;

    String nodeExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException;

    String relationshipExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException;

    void nodeAddToExplicitIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException;

    void nodeRemoveFromExplicitIndex( String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException;

    void nodeRemoveFromExplicitIndex( String indexName, long node, String key ) throws
            ExplicitIndexNotFoundKernelException;

    void nodeRemoveFromExplicitIndex( String indexName, long node ) throws ExplicitIndexNotFoundKernelException;

    void relationshipAddToExplicitIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException;

    void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key, Object value )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

    void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

    void relationshipRemoveFromExplicitIndex( String indexName, long relationship )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException;

    void nodeExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException;

    void relationshipExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException;
}
