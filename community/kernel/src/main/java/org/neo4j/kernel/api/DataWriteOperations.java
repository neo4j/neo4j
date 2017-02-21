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
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

public interface DataWriteOperations
{
    //===========================================
    //== DATA OPERATIONS ========================
    //===========================================

    long nodeCreate();

    void nodeDelete( long nodeId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException;

    int nodeDetachDelete( long nodeId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException, KernelException;

    long relationshipCreate( int relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException;

    void relationshipDelete( long relationshipId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException;

    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link org.neo4j.kernel.impl.api.operations.KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement,
     * String)} or {@link
     * org.neo4j.kernel.impl.api.operations.KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationException;

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link org.neo4j.kernel.impl.api.operations.KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement,
     * String)} or {@link
     * org.neo4j.kernel.impl.api.operations.KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    Property nodeSetProperty( long nodeId, DefinedProperty property )
            throws EntityNotFoundException, AutoIndexingKernelException,
                    InvalidTransactionTypeKernelException, ConstraintValidationException;

    Property relationshipSetProperty( long relationshipId, DefinedProperty property )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException;

    Property graphSetProperty( DefinedProperty property );

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Property nodeRemoveProperty( long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException;

    Property relationshipRemoveProperty( long relationshipId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException;

    Property graphRemoveProperty( int propertyKeyId );

    /**
     * Creates a legacy index in a separate transaction if not yet available.
     */
    void nodeLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig );

    void nodeLegacyIndexCreate( String indexName, Map<String, String> customConfig );

    //===========================================
    //== LEGACY INDEX OPERATIONS ================
    //===========================================

    /**
     * Creates a legacy index in a separate transaction if not yet available.
     */
    void relationshipLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig );

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
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException;

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException;

    void relationshipRemoveFromLegacyIndex( String indexName, long relationship )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException;

    void nodeLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException;

    void relationshipLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException;
}
