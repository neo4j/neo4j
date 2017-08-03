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
package org.neo4j.storageengine.api;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.schema.PopulationProgress;

/**
 * Abstraction for reading committed data from {@link StorageEngine store}.
 */
public interface StoreReadLayer
{
    /**
     * @return new {@link StorageStatement}, which can be used after a call to {@link StorageStatement#acquire()}.
     */
    StorageStatement newStatement();

    /**
     * @param labelId label to list indexes for.
     * @return {@link IndexDescriptor} associated with the given {@code labelId}.
     */
    Iterator<IndexDescriptor> indexesGetForLabel( int labelId );

    /**
     * @return all {@link IndexDescriptor} in storage.
     */
    Iterator<IndexDescriptor> indexesGetAll();

    /**
     * Returns all indexes (including unique) related to a property.
     */
    Iterator<IndexDescriptor> indexesGetRelatedToProperty( int propertyId );

    /**
     * @param index {@link IndexDescriptor} to get related uniqueness constraint for.
     * @return schema rule id of uniqueness constraint that owns the given {@code index}, or {@code null}
     * if the given index isn't related to a uniqueness constraint.
     */
    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index );

    /**
     * @param index {@link IndexDescriptor} to get schema rule id for.
     * @return schema rule id for matching index.
     * @throws SchemaRuleNotFoundException if no such index exists in storage.
     */
    long indexGetCommittedId( IndexDescriptor index )
            throws SchemaRuleNotFoundException;

    /**
     * @return iterator with property keys of all stored graph properties.
     */
    PrimitiveIntIterator graphGetPropertyKeys();

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

    PrimitiveLongIterator nodesGetForLabel( StorageStatement statement, int labelId );

    /**
     * Looks for a stored index by given {@code descriptor}
     *
     * @param descriptor a description of the index.
     * @return {@link IndexDescriptor} for matching index, or {@code null} if not found.
     */
    IndexDescriptor indexGetForSchema( LabelSchemaDescriptor descriptor );

    /**
     * Returns state of a stored index.
     *
     * @param descriptor {@link LabelSchemaDescriptor} to get state for.
     * @return {@link InternalIndexState} for index.
     * @throws IndexNotFoundKernelException if index not found.
     */
    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * @param descriptor {@link LabelSchemaDescriptor} to get population progress for.
     * @return progress of index population, which is the initial state of an index when it's created.
     * @throws IndexNotFoundKernelException if index not found.
     */
    PopulationProgress indexGetPopulationProgress( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns any failure that happened during population or operation of an index. Such failures
     * are persisted and can be accessed even after restart.
     *
     * @param descriptor {@link LabelSchemaDescriptor} to get failure for.
     * @return failure of an index, or {@code null} if index is working as it should.
     * @throws IndexNotFoundKernelException if index not found.
     */
    String indexGetFailure( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

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
    String labelGetName( int labelId ) throws LabelNotFoundKernelException;

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
    PrimitiveLongIterator nodesGetAll();

    /**
     * @return ids of all stored relationships. The returned iterator can optionally visit data about
     * each relationship returned.
     */
    RelationshipIterator relationshipsGetAll();

    Cursor<RelationshipItem> nodeGetRelationships( StorageStatement statement, NodeItem nodeItem, Direction direction );

    Cursor<RelationshipItem> nodeGetRelationships( StorageStatement statement, NodeItem nodeItem, Direction direction,
            IntPredicate typeIds );

    Cursor<PropertyItem> nodeGetProperties( StorageStatement statement, NodeItem node, AssertOpen assertOpen );

    Cursor<PropertyItem> nodeGetProperty( StorageStatement statement, NodeItem node, int propertyKeyId,
            AssertOpen assertOpen );

    Cursor<PropertyItem> relationshipGetProperties( StorageStatement statement, RelationshipItem relationship,
            AssertOpen assertOpen );

    Cursor<PropertyItem> relationshipGetProperty( StorageStatement statement, RelationshipItem relationshipItem,
            int propertyKeyId, AssertOpen assertOpen );

    /**
     * Releases a previously {@link StorageStatement#reserveNode() reserved} node id if it turns out to not actually being used,
     * for example in the event of a transaction rolling back.
     *
     * @param id reserved node id to release.
     */
    void releaseNode( long id );

    /**
     * Releases a previously {@link StorageStatement#reserveRelationship() reserved} relationship id if it turns out to not
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
     * @param descriptor {@link LabelSchemaDescriptor} to return size for.
     * @return number of entities in the given index.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    long indexSize( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns percentage of values in the given {@code index} are unique. A value of {@code 1.0} means that
     * all values in the index are unique, e.g. that there are no duplicate values. A value of, say {@code 0.9}
     * means that 10% of the values are duplicates.
     *
     * @param descriptor {@link LabelSchemaDescriptor} to get uniqueness percentage for.
     * @return percentage of values being unique in this index, max {@code 1.0} for all unique.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    double indexUniqueValuesPercentage( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    long nodesGetCount();

    long relationshipsGetCount();

    int labelCount();

    int propertyKeyCount();

    int relationshipTypeCount();

    DoubleLongRegister indexUpdatesAndSize( LabelSchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    DoubleLongRegister indexSample( LabelSchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    boolean nodeExists( long id );

    PrimitiveIntSet relationshipTypes( StorageStatement statement, NodeItem node );

    void degrees( StorageStatement statement, NodeItem nodeItem, DegreeVisitor visitor );

    int degreeRelationshipsInGroup( StorageStatement storeStatement, long id, long groupId, Direction direction,
            Integer relType );

    <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StoreReadLayer, T> factory );
}
