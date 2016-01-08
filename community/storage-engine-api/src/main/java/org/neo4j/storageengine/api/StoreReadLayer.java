/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.schema.IndexPopulationProgress;
import org.neo4j.storageengine.api.schema.IndexSchemaRule;
import org.neo4j.storageengine.api.schema.SchemaRule;

/**
 * Abstraction for reading committed data from {@link StorageEngine store}.
 */
public interface StoreReadLayer
{
    /**
     * @return statement acquired and held onto by caller. Contains data structures which are beneficial to reuse
     * within a certain context.
     */
    StorageStatement acquireStatement();

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
     * @param labelId label to list indexes related to uniqueness constraints for.
     * @return {@link IndexDescriptor} related to uniqueness constraints associated with the given {@code labelId}.
     */
    Iterator<IndexDescriptor> uniquenessIndexesGetForLabel( int labelId );

    /**
     * @return all {@link IndexDescriptor} related to uniqueness constraints.
     */
    Iterator<IndexDescriptor> uniquenessIndexesGetAll();

    /**
     * @param index {@link IndexDescriptor} to get related uniqueness constraint for.
     * @return schema rule id of uniqueness constraint that owns the given {@code index}, or {@code null}
     * if the given index isn't related to a uniqueness constraint.
     * @throws SchemaRuleNotFoundException if there's no such index matching the given {@code index} in storage.
     */
    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
            throws SchemaRuleNotFoundException;

    /**
     * @param index {@link IndexDescriptor} to get schema rule id for.
     * @param filter for type of index to match.
     * @return schema rule id for matching index.
     * @throws SchemaRuleNotFoundException if no such index exists in storage.
     */
    long indexGetCommittedId( IndexDescriptor index, Predicate<SchemaRule.Kind> filter )
            throws SchemaRuleNotFoundException;

    /**
     * @param index {@link IndexDescriptor} to get index schema rule for.
     * @param filter for type of index to match.
     * @return index schema rule for matching index.
     * @throws SchemaRuleNotFoundException if no such index exists in storage.
     */
    IndexSchemaRule indexRule( IndexDescriptor index, Predicate<SchemaRule.Kind> filter )
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
     * @param labelId label token id .
     * @param propertyKeyId property key token id.
     * @return node property constraints associated with the label and property key token ids.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId );

    /**
     * @param labelId label token id .
     * @return node property constraints associated with the label token id.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId );

    /**
     * @param typeId relationship type token id .
     * @param propertyKeyId property key token id.
     * @return relationship property constraints associated with the relationship type and property key token ids.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId,
            int propertyKeyId );

    /**
     * @param typeId relationship type token id .
     * @return relationship property constraints associated with the relationship type token id.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId );

    /**
     * @return all stored property constraints.
     */
    Iterator<PropertyConstraint> constraintsGetAll();

    PrimitiveLongIterator nodesGetForLabel( StorageStatement statement, int labelId );

//    /**
//     * Searches an index backing a uniqueness constraint for a certain value.
//     *
//     * @param statement {@link StorageStatement} created using {@link #acquireStatement()}.
//     * @param index {@link IndexDescriptor} specifying index to search.
//     * @param value property value to search for.
//     * @return ids of matching nodes.
//     * @throws IndexNotFoundKernelException if {@code index} doesn't exist.
//     * @throws IndexBrokenKernelException if index is broken and can't be used.
//     */
//    PrimitiveLongResourceIterator nodeGetFromUniqueIndexSeek( StorageStatement statement, IndexDescriptor index,
//            Object value )
//            throws IndexNotFoundKernelException, IndexBrokenKernelException;
//
//    /**
//     * Searches an index for a certain value.
//     *
//     * @param statement {@link StorageStatement} created using {@link #acquireStatement()}.
//     * @param index {@link IndexDescriptor} specifying index to search.
//     * @param value property value to search for.
//     * @return ids of matching nodes.
//     * @throws IndexNotFoundKernelException if {@code index} doesn't exist.
//     */
//    PrimitiveLongIterator nodesGetFromIndexSeek( StorageStatement statement, IndexDescriptor index, Object value )
//            throws IndexNotFoundKernelException;
//
//    /**
//     * Searches an index for numerics values between {@code lower} and {@code upper}.
//     *
//     * @param statement {@link StorageStatement} created using {@link #acquireStatement()}.
//     * @param index {@link IndexDescriptor} specifying index to search.
//     * @param lower lower numeric bound of search (inclusive).
//     * @param upper upper numeric bound of search (inclusive).
//     * @return ids of matching nodes.
//     * @throws IndexNotFoundKernelException if {@code index} doesn't exist.
//     */
//    PrimitiveLongIterator nodesGetFromInclusiveNumericIndexRangeSeek( StorageStatement statement,
//            IndexDescriptor index, Number lower, Number upper )
//            throws IndexNotFoundKernelException;
//
//    /**
//     * Searches an index for string values between {@code lower} and {@code upper}.
//     *
//     * @param statement {@link StorageStatement} created using {@link #acquireStatement()}.
//     * @param index {@link IndexDescriptor} specifying index to search.
//     * @param lower lower numeric bound of search.
//     * @param includeLower whether or not lower bound is inclusive.
//     * @param upper upper numeric bound of search.
//     * @param includeUpper whether or not upper bound is inclusive.
//     * @return ids of matching nodes.
//     * @throws IndexNotFoundKernelException if {@code index} doesn't exist.
//     */
//    PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( StorageStatement statement, IndexDescriptor index,
//            String lower, boolean includeLower, String upper, boolean includeUpper )
//            throws IndexNotFoundKernelException;
//
//    /**
//     * Searches an index for string values starting with {@code prefix}.
//     *
//     * @param statement {@link StorageStatement} created using {@link #acquireStatement()}.
//     * @param index {@link IndexDescriptor} specifying index to search.
//     * @param prefix prefix that matching strings must start with.
//     * @return ids of matching nodes.
//     * @throws IndexNotFoundKernelException if {@code index} doesn't exist.
//     */
//    PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( StorageStatement statement,
//            IndexDescriptor index, String prefix )
//            throws IndexNotFoundKernelException;
//
//    /**
//     * Scans an index returning all nodes.
//     *
//     * @param statement {@link StorageStatement} created using {@link #acquireStatement()}.
//     * @param index {@link IndexDescriptor} specifying index to search.
//     * @return node ids in index.
//     * @throws IndexNotFoundKernelException if {@code index} doesn't exist.
//     */
//    PrimitiveLongIterator nodesGetFromIndexScan( StorageStatement statement, IndexDescriptor index )
//            throws IndexNotFoundKernelException;

    /**
     * Looks for a stored index by given {@code labelId} and {@code propertyKey}
     *
     * @param labelId label id.
     * @param propertyKeyId property key id.
     * @return {@link IndexDescriptor} for matching index, or {@code null} if not found. TODO should throw exception.
     */
    IndexDescriptor indexGetForLabelAndPropertyKey( int labelId, int propertyKeyId );

    /**
     * Returns state of a stored index.
     *
     * @param index {@link IndexDescriptor} to get state for.
     * @return {@link InternalIndexState} for index.
     * @throws IndexNotFoundKernelException if index not found.
     */
    InternalIndexState indexGetState( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * @param index {@link IndexDescriptor} to get population progress for.
     * @return progress of index population, which is the initial state of an index when it's created.
     * @throws IndexNotFoundKernelException if index not found.
     */
    IndexPopulationProgress indexGetPopulationProgress( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Returns any failure that happened during population or operation of an index. Such failures
     * are persisted and can be accessed even after restart.
     *
     * @param index {@link IndexDescriptor} to get failure for.
     * @return failure of an index, or {@code null} if index is working as it should.
     * @throws IndexNotFoundKernelException if index not found.
     */
    String indexGetFailure( IndexDescriptor index ) throws IndexNotFoundKernelException;

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
     * @param index {@link IndexDescriptor} to return size for.
     * @return number of entities in the given index.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    long indexSize( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Returns percentage of values in the given {@code index} are unique. A value of {@code 1.0} means that
     * all values in the index are unique, e.g. that there are no duplicate values. A value of, say {@code 0.9}
     * means that 10% of the values are duplicates.
     *
     * @param index {@link IndexDescriptor} to get uniqueness percentage for.
     * @return percentage of values being unique in this index, max {@code 1.0} for all unique.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    double indexUniqueValuesPercentage( IndexDescriptor index ) throws IndexNotFoundKernelException;
}
