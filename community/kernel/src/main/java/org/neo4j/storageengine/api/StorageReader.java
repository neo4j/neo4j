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

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.IntSet;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.IntPredicate;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;

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
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @param relationshipId id of relationship to get cursor for.
     * @return a {@link Cursor} over {@link RelationshipItem} for the given {@code relationshipId}.
     */
    Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relationshipId );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @param isDense if the node is dense
     * @param nodeId the id of the node where to start traversing the relationships
     * @param relationshipId the id of the first relationship in the chain
     * @param direction the direction of the relationship wrt the node
     * @param relTypeFilter the allowed types (it allows all types if unspecified)
     * @return a {@link Cursor} over {@link RelationshipItem} for traversing the relationships associated to the node.
     */
    Cursor<RelationshipItem> acquireNodeRelationshipCursor(  boolean isDense, long nodeId, long relationshipId,
            Direction direction, IntPredicate relTypeFilter );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @return a {@link Cursor} over all stored relationships.
     */
    Cursor<RelationshipItem> relationshipsGetAllCursor();

    Cursor<PropertyItem> acquirePropertyCursor( long propertyId, Lock shortLivedReadLock, AssertOpen assertOpen );

    Cursor<PropertyItem> acquireSinglePropertyCursor( long propertyId, int propertyKeyId, Lock shortLivedReadLock,
            AssertOpen assertOpen );

    /**
     * @return {@link LabelScanReader} capable of reading nodes for specific label ids.
     */
    LabelScanReader getLabelScanReader();

    /**
     * Returns an {@link IndexReader} for searching entity ids given property values. One reader is allocated
     * and kept per index throughout the life of a statement, making the returned reader repeatable-read isolation.
     * <p>
     * <b>NOTE:</b>
     * Reader returned from this method should not be closed. All such readers will be closed during {@link #close()}
     * of the current statement.
     *
     * @param index {@link SchemaIndexDescriptor} to get reader for.
     * @return {@link IndexReader} capable of searching entity ids given property values.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    IndexReader getIndexReader( SchemaIndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Returns an {@link IndexReader} for searching entity ids given property values. A new reader is allocated
     * every call to this method, which means that newly committed data since the last call to this method
     * will be visible in the returned reader.
     * <p>
     * <b>NOTE:</b>
     * It is caller's responsibility to close the returned reader.
     *
     * @param index {@link SchemaIndexDescriptor} to get reader for.
     * @return {@link IndexReader} capable of searching entity ids given property values.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    IndexReader getFreshIndexReader( SchemaIndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Access to low level record cursors
     *
     * @return record cursors
     */
    RecordCursors recordCursors();

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

    long getGraphPropertyReference();

    /**
     * Returns all indexes (including unique) related to a property.
     */
    Iterator<SchemaIndexDescriptor> indexesGetRelatedToProperty( int propertyId );

    /**
     * @param index {@link SchemaIndexDescriptor} to get related uniqueness constraint for.
     * @return schema rule id of uniqueness constraint that owns the given {@code index}, or {@code null}
     * if the given index isn't related to a uniqueness constraint.
     */
    Long indexGetOwningUniquenessConstraintId( SchemaIndexDescriptor index );

    /**
     * @param index {@link SchemaIndexDescriptor} to get schema rule id for.
     * @return schema rule id for matching index.
     * @throws SchemaRuleNotFoundException if no such index exists in storage.
     */
    long indexGetCommittedId( SchemaIndexDescriptor index )
            throws SchemaRuleNotFoundException;

    /**
     * @return iterator with property keys of all stored graph properties.
     */
    IntIterator graphGetPropertyKeys();

    /**
     * @param propertyKeyId property key id to get graph property for.
     * @return property value of graph property with key {@code propertyKeyId}, or {@code null} if not found.
     */
    Object graphGetProperty( int propertyKeyId );

    /**
     * @return all stored graph properties.
     */
    Iterator<StorageProperty> graphGetAllProperties();

    /**
     * @param descriptor describing the label and property key (or keys) defining the requested constraint.
     * @return node property constraints associated with the label and one or more property keys token ids.
     */
    Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor );

    boolean constraintExists( ConstraintDescriptor descriptor );

    /**
     * @param labelId label token id.
     * @return node property constraints associated with the label token id.
     */
    Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId );

    /**
     * @param typeId relationship type token id .
     * @return relationship property constraints associated with the relationship type token id.
     */
    Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId );

    /**
     * @return all stored property constraints.
     */
    Iterator<ConstraintDescriptor> constraintsGetAll();

    /**
     *
     * @param labelId The label id of interest.
     * @return {@link PrimitiveLongResourceIterator} over node ids associated with given label id.
     */
    PrimitiveLongResourceIterator nodesGetForLabel( int labelId );

    /**
     * Looks for a stored index by given {@code descriptor}
     *
     * @param descriptor a description of the index.
     * @return {@link SchemaIndexDescriptor} for matching index, or {@code null} if not found.
     */
    SchemaIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor );

    /**
     * Returns state of a stored index.
     *
     * @param descriptor {@link SchemaIndexDescriptor} to get state for.
     * @return {@link InternalIndexState} for index.
     * @throws IndexNotFoundKernelException if index not found.
     */
    InternalIndexState indexGetState( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Return index provider descriptor of a stored index.
     *
     * @param descriptor {@link SchemaIndexDescriptor} to get provider descriptor for.
     * @return {@link IndexProvider.Descriptor} for index.
     * @throws IndexNotFoundKernelException if index not found.
     */
    IndexProvider.Descriptor indexGetProviderDescriptor( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Return index reference of a stored index.
     *
     * @param descriptor {@link SchemaIndexDescriptor} to get provider reference for.
     * @return {@link IndexProvider.Descriptor} for index.
     * @throws IndexNotFoundKernelException if index not found.
     */
    CapableIndexReference indexReference( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * @param descriptor {@link SchemaDescriptor} to get population progress for.
     * @return progress of index population, which is the initial state of an index when it's created.
     * @throws IndexNotFoundKernelException if index not found.
     */
    PopulationProgress indexGetPopulationProgress( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns any failure that happened during population or operation of an index. Such failures
     * are persisted and can be accessed even after restart.
     *
     * @param descriptor {@link SchemaDescriptor} to get failure for.
     * @return failure of an index, or {@code null} if index is working as it should.
     * @throws IndexNotFoundKernelException if index not found.
     */
    String indexGetFailure( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

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
     * Visits data about a relationship. The given {@code relationshipVisitor} will be notified.
     *
     * @param relationshipId the id of the relationship to access.
     * @param relationshipVisitor {@link RelationshipVisitor} which will see the relationship data.
     * @throws EntityNotFoundException if no relationship exists by the given {@code relationshipId}.
     */
    <EXCEPTION extends Exception> void relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> relationshipVisitor ) throws EntityNotFoundException, EXCEPTION;

    /**
     * @return ids of all stored nodes.
     */
    LongIterator nodesGetAll();

    /**
     * @return ids of all stored relationships. The returned iterator can optionally visit data about
     * each relationship returned.
     */
    RelationshipIterator relationshipsGetAll();

    Cursor<RelationshipItem> nodeGetRelationships( NodeItem nodeItem, Direction direction );

    Cursor<RelationshipItem> nodeGetRelationships( NodeItem nodeItem, Direction direction,
            IntPredicate typeIds );

    Cursor<PropertyItem> nodeGetProperties( NodeItem node, AssertOpen assertOpen );

    Cursor<PropertyItem> nodeGetProperty( NodeItem node, int propertyKeyId,
            AssertOpen assertOpen );

    Cursor<PropertyItem> relationshipGetProperties( RelationshipItem relationship,
            AssertOpen assertOpen );

    Cursor<PropertyItem> relationshipGetProperty( RelationshipItem relationshipItem,
            int propertyKeyId, AssertOpen assertOpen );

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

    /**
     * Returns number of stored nodes labeled with the label represented by {@code labelId}.
     *
     * @param labelId label id to match.
     * @return number of stored nodes with this label.
     */
    long countsForNode( int labelId );

    /**
     * Returns number of stored relationships of a certain {@code typeId} whose start/end nodes are labeled
     * with the {@code startLabelId} and {@code endLabelId} respectively.
     *
     * @param startLabelId label id of start nodes to match.
     * @param typeId relationship type id to match.
     * @param endLabelId label id of end nodes to match.
     * @return number of stored relationships matching these criteria.
     */
    long countsForRelationship( int startLabelId, int typeId, int endLabelId );

    /**
     * Returns size of index, i.e. number of entities in that index.
     *
     * @param descriptor {@link SchemaDescriptor} to return size for.
     * @return number of entities in the given index.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    long indexSize( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns percentage of values in the given {@code index} are unique. A value of {@code 1.0} means that
     * all values in the index are unique, e.g. that there are no duplicate values. A value of, say {@code 0.9}
     * means that 10% of the values are duplicates.
     *
     * @param descriptor {@link SchemaDescriptor} to get uniqueness percentage for.
     * @return percentage of values being unique in this index, max {@code 1.0} for all unique.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    double indexUniqueValuesPercentage( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    long nodesGetCount();

    long relationshipsGetCount();

    int labelCount();

    int propertyKeyCount();

    int relationshipTypeCount();

    DoubleLongRegister indexUpdatesAndSize( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    DoubleLongRegister indexSample( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    boolean nodeExists( long id );

    boolean relationshipExists( long id );

    IntSet relationshipTypes( NodeItem node );

    void degrees( NodeItem nodeItem, DegreeVisitor visitor );

    int degreeRelationshipsInGroup( long id, long groupId, Direction direction, Integer relType );

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
