/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.storageengine.api;

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.register.Register.DoubleLongRegister;

/**
 * Abstraction for accessing data from a {@link StorageEngine}.
 * <p>
 * A {@link StorageReader} must be {@link #initialize(TransactionalDependencies) initialized} before use in a
 * and {@link #acquire() acquired} before use in a statement, followed by {@link #release()} after statement is completed.
 * <p>
 * Creating and closing {@link StorageReader} can be somewhat costly, so there are benefits keeping these readers open
 * during a longer period of time, with the assumption that it's still one thread at a time using each.
 */
public interface StorageReader extends AutoCloseable, Read, ExplicitIndexRead, SchemaRead, CursorFactory
{
    /**
     * Initializes some dependencies that this reader needs. Typically called once per transaction.
     *
     * @param dependencies {@link TransactionalDependencies} needed to implement transaction-aware cursors.
     */
    void initialize( TransactionalDependencies dependencies );

    /**
     * Acquires this statement so that it can be used, should later be {@link #release() released}.
     * Typically called once per statement.
     * Since a {@link StorageReader} can be reused after {@link #release() released}, this call should
     * do initialization/clearing of state whereas data structures can be kept between uses.
     */
    void acquire();

    /**
     * Releases resources tied to this statement and makes this reader able to be {@link #acquire() acquired} again.
     */
    void release();

    /**
     * Closes this reader and all resources so that it can no longer be used nor {@link #acquire() acquired}.
     */
    @Override
    void close();

    /**
     * Reserves a node id for future use to store a node. The reason for it being exposed here is that
     * internal ids of nodes and relationships are publicly accessible all the way out to the user.
     * This will likely change in the future though.
     *
     * @return a reserved node id for future use.
     */
    long reserveNode();

    /**
     * Reserves a relationship id for future use to store a relationship. The reason for it being exposed here is that
     * internal ids of nodes and relationships are publicly accessible all the way out to the user.
     * This will likely change in the future though.
     *
     * @return a reserved relationship id for future use.
     */
    long reserveRelationship();

    /**
     * Returns all indexes (including unique) related to a property.
     */
    Iterator<SchemaIndexDescriptor> indexesGetRelatedToProperty( int propertyId );

    /**
     * @param labelName name of label.
     * @return token id of label.
     */
    int labelGetForName( String labelName );

    /**
     * @param labelId label id to get name for.
     * @return label name for given label id.
     * @throws LabelNotFoundKernelException if no label by {@code labelId} was found.
     */
    String labelGetName( long labelId ) throws LabelNotFoundKernelException;

    /**
     * @param propertyKeyName name of property key.
     * @return token id of property key.
     */
    int propertyKeyGetForName( String propertyKeyName );

    /**
     * Gets property key token id for the given {@code propertyKeyName}, or creates one if there is no
     * existing property key with the given name.
     *
     * @param propertyKeyName name of property key.
     * @return property key token id for the given name, created if need be.
     */
    int propertyKeyGetOrCreateForName( String propertyKeyName );

    /**
     * @param propertyKeyId property key to get name for.
     * @return property key name for given property key id.
     * @throws PropertyKeyIdNotFoundKernelException if no property key by {@code propertyKeyId} was found.
     */
    String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException;

    /**
     * @return all stored property key tokens.
     */
    Iterator<Token> propertyKeyGetAllTokens();

    /**
     * @return all stored label tokens.
     */
    Iterator<Token> labelsGetAllTokens();

    /**
     * @return all stored relationship type tokens.
     */
    Iterator<Token> relationshipTypeGetAllTokens();

    /**
     * @param relationshipTypeName name of relationship type.
     * @return token id of relationship type.
     */
    int relationshipTypeGetForName( String relationshipTypeName );

    /**
     * @param relationshipTypeId relationship type id to get name for.
     * @return relationship type name of given relationship type id.
     * @throws RelationshipTypeIdNotFoundKernelException if no label by {@code labelId} was found.
     */
    String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException;

    /**
     * Gets label token id for the given {@code labelName}, or creates one if there is no
     * existing label with the given name.
     *
     * @param labelName name of label.
     * @return label token id for the given name, created if need be.
     * @throws TooManyLabelsException if creating this label would have exceeded storage limitations for
     * number of stored labels.
     */
    int labelGetOrCreateForName( String labelName ) throws TooManyLabelsException;

    /**
     * Gets relationship type token id for the given {@code relationshipTypeName}, or creates one if there is no
     * existing relationship type with the given name.
     *
     * @param relationshipTypeName name of relationship type.
     * @return relationship type token id for the given name, created if need be.
     */
    int relationshipTypeGetOrCreateForName( String relationshipTypeName );

    /**
     * Releases a previously {@link #reserveNode() reserved} node id if it turns out to not actually being used,
     * for example in the event of a transaction rolling back.
     *
     * @param id reserved node id to release.
     */
    void releaseNode( long id );

    /**
     * Releases a previously {@link #reserveRelationship() reserved} relationship id if it turns out to not
     * actually being used, for example in the event of a transaction rolling back.
     *
     * @param id reserved relationship id to release.
     */
    void releaseRelationship( long id );

    int labelCount();

    int propertyKeyCount();

    int relationshipTypeCount();

    <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader, T> factory );

    /*
     * Allocates cursors which are transaction-state-unaware
     *
     * Reading from storage in combination with transaction-state is a bit entangle now, since the introduction of the new kernel API,
     * where transaction-state awareness happens inside the actual cursors. The current approach is to disable transaction-state
     * awareness per cursor so that those few places that need reading only from storage will allocate cursors using the methods below.
     */

    NodeCursor allocateNodeCursorCommitted();

    PropertyCursor allocatePropertyCursorCommitted();

    RelationshipScanCursor allocateRelationshipScanCursorCommitted();

    RelationshipGroupCursor allocateRelationshipGroupCursorCommitted();
}
