/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.txstate;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.RelationshipState;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

/**
 * Kernel transaction state.
 *
 * This interface contains the methods for reading the state from the transaction state. The implementation of these
 * methods should be free of any side effects (such as initialising lazy state). Modifying methods are found in the
 * {@link TransactionState} interface.
 */
public interface ReadableTxState
{
    void accept( TxStateVisitor visitor );

    boolean hasChanges();

    // ENTITY RELATED

    /** Returns all nodes that, in this tx, have had labelId removed. */
    ReadableDiffSets<Long> nodesWithLabelChanged( int labelId );

    /** Returns nodes that have been added and removed in this tx. */
    ReadableDiffSets<Long> addedAndRemovedNodes();

    /** Returns rels that have been added and removed in this tx. */
    ReadableDiffSets<Long> addedAndRemovedRelationships();

    /** Nodes that have had labels, relationships, or properties modified in this tx. */
    Iterable<NodeState> modifiedNodes();

    /** Rels that have properties modified in this tx. */
    Iterable<RelationshipState> modifiedRelationships();

    boolean relationshipIsAddedInThisTx( long relationshipId );

    boolean relationshipIsDeletedInThisTx( long relationshipId );

    ReadableDiffSets<Long> nodesWithChangedProperty( int propertyKeyId, Object value );

    ReadableDiffSets<Integer> nodeStateLabelDiffSets( long nodeId );

    Iterator<DefinedProperty> augmentNodeProperties( long nodeId, Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> augmentRelationshipProperties( long relId, Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> augmentGraphProperties( Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> addedAndChangedNodeProperties( long nodeId );

    Iterator<DefinedProperty> addedAndChangedRelationshipProperties( long relId );

    UpdateTriState labelState( long nodeId, int labelId );

    boolean nodeIsAddedInThisTx( long nodeId );

    boolean nodeIsDeletedInThisTx( long nodeId );

    boolean nodeModifiedInThisTx( long nodeId );

    // TODO: refactor so that these are the same!
    PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, PrimitiveLongIterator stored );

    PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, int[] relTypes,
                                                PrimitiveLongIterator stored );

    PrimitiveLongIterator addedRelationships( long nodeId, int[] relTypes, Direction direction );

    PrimitiveIntIterator nodeRelationshipTypes( long nodeId );

    int augmentNodeDegree( long node, int committedDegree, Direction direction );

    int augmentNodeDegree( long node, int committedDegree, Direction direction, int relType );

    PrimitiveLongIterator augmentNodesGetAll( PrimitiveLongIterator committed );

    PrimitiveLongIterator augmentRelationshipsGetAll( PrimitiveLongIterator committed );

    /**
     * @return {@code true} if the relationship was visited in this state, i.e. if it was created
     * by this current transaction, otherwise {@code false} where the relationship might need to be
     * visited from the store.
     */
    <EX extends Exception> boolean relationshipVisit( long relId, RelationshipVisitor<EX> visitor ) throws EX;

    // SCHEMA RELATED

    ReadableDiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId );

    ReadableDiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId );

    ReadableDiffSets<IndexDescriptor> indexChanges();

    ReadableDiffSets<IndexDescriptor> constraintIndexChanges();

    Iterable<IndexDescriptor> constraintIndexesCreatedInTx();

    ReadableDiffSets<UniquenessConstraint> constraintsChanges();

    ReadableDiffSets<UniquenessConstraint> constraintsChangesForLabel( int labelId );

    ReadableDiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( int labelId, int propertyKey );

    Long indexCreatedForConstraint( UniquenessConstraint constraint );

    ReadableDiffSets<Long> indexUpdates( IndexDescriptor index, Object value );

    /**
     * The way tokens are created is that the first time a token is needed it gets created in its own little
     * token mini-transaction, separate from the surrounding transaction that creates or modifies data that need it.
     * From the kernel POV it's interesting to know whether or not any tokens have been created in this tx state,
     * because then we know it's a mini-transaction like this and won't have to let transaction event handlers
     * know about it, for example.
     *
     * The same applies to schema changes, such as creating and dropping indexes and constraints.
     */
    boolean hasDataChanges();
}
