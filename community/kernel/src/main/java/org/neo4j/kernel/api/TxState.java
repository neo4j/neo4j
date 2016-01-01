/**
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
package org.neo4j.kernel.api;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.RelationshipState;
import org.neo4j.kernel.impl.util.DiffSets;

/**
 * Kernel transaction state, please see {@link org.neo4j.kernel.impl.api.state.TxStateImpl} for details.
 *
 * The naming of methods in this class roughly follows the naming of {@link org.neo4j.kernel.api.Statement}
 * with one exception: All transaction state mutators must include the particle "Do" in their name, e.g.
 * nodeDoAdd. This helps deciding where to set "hasChanges" in the main implementation class {@link org.neo4j.kernel.impl.api.state.TxStateImpl}.
 */
public interface TxState
{
    public enum UpdateTriState
    {
        ADDED
        {
            @Override
            public boolean isTouched()
            {
                return true;
            }

            @Override
            public boolean isAdded()
            {
                return true;
            }
        },
        REMOVED
        {
            @Override
            public boolean isTouched()
            {
                return true;
            }

            @Override
            public boolean isAdded()
            {
                return false;
            }
        },
        UNTOUCHED
        {
            @Override
            public boolean isTouched()
            {
                return false;
            }

            @Override
            public boolean isAdded()
            {
                throw new UnsupportedOperationException( "Cannot convert an UNTOUCHED UpdateTriState to a boolean" );
            }
        };

        public abstract boolean isTouched();

        public abstract boolean isAdded();
    }

    public interface Holder
    {

        TxState txState();
        boolean hasTxState();

        boolean hasTxStateWithChanges();

    }
    /**
     * Ability to generate the leaking id types (node ids and relationship ids).
     */
    public interface IdGeneration
    {

        long newNodeId();
        long newRelationshipId();

    }

    public interface Visitor
    {
        void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                       Iterator<Integer> removed );

        void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                      Iterator<Integer> removed );

        void visitGraphPropertyChanges( Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                        Iterator<Integer> removed );

        void visitNodeLabelChanges( long id, Iterator<Integer> added, Iterator<Integer> removed );

        void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitAddedConstraint( UniquenessConstraint element );

        void visitRemovedConstraint( UniquenessConstraint element );
    }

    boolean hasChanges();

    void accept( Visitor visitor );


    // ENTITY RELATED

    long relationshipDoCreate( int relationshipTypeId, long startNodeId, long endNodeId );

    long nodeDoCreate();

    DiffSets<Long> labelStateNodeDiffSets( int labelId );

    DiffSets<Integer> nodeStateLabelDiffSets( long nodeId );

    Iterator<DefinedProperty> augmentNodeProperties( long nodeId, Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> augmentRelProperties( long relId, Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> augmentGraphProperties( Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> addedAndChangedNodeProperties( long nodeId );

    Iterator<DefinedProperty> addedAndChangedRelProperties( long relId );

    /** Returns all nodes that, in this tx, have had labelId added. */
    Set<Long> nodesWithLabelAdded( int labelId );

    /** Returns all nodes that, in this tx, have had labelId removed.  */
    DiffSets<Long> nodesWithLabelChanged( int labelId );

    /** Returns nodes that have been added and removed in this tx. */
    DiffSets<Long> addedAndRemovedNodes();

    /** Returns rels that have been added and removed in this tx. */
    DiffSets<Long> addedAndRemovedRels();

    /** Nodes that have had labels, relationships, or properties modified in this tx. */
    Iterable<NodeState> modifiedNodes();

    /** Rels that have properties modified in this tx. */
    Iterable<RelationshipState> modifiedRelationships();

    boolean nodeIsAddedInThisTx( long nodeId );

    boolean nodeIsDeletedInThisTx( long nodeId );

    boolean nodeModifiedInThisTx( long nodeId );

    DiffSets<Long> nodesWithChangedProperty( int propertyKeyId, Object value );

    boolean relationshipIsAddedInThisTx( long relationshipId );

    boolean relationshipIsDeletedInThisTx( long relationshipId );

    UpdateTriState labelState( long nodeId, int labelId );

    void relationshipDoDelete( long relationshipId, long startNode, long endNode, int type );

    void relationshipDoDeleteAddedInThisTx( long relationshipId );

    void nodeDoDelete( long nodeId );

    void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty );

    void relationshipDoReplaceProperty( long relationshipId,
                                        Property replacedProperty, DefinedProperty newProperty );

    void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty );

    void nodeDoRemoveProperty( long nodeId, DefinedProperty removedProperty );

    void relationshipDoRemoveProperty( long relationshipId, DefinedProperty removedProperty );

    void graphDoRemoveProperty( DefinedProperty removedProperty );

    void nodeDoAddLabel( int labelId, long nodeId );

    void nodeDoRemoveLabel( int labelId, long nodeId );

    PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, PrimitiveLongIterator stored );

    PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, int[] relTypes,
                                                PrimitiveLongIterator stored );

    int augmentNodeDegree( long node, int committedDegree, Direction direction );

    int augmentNodeDegree( long node, int committedDegree, Direction direction, int relType );

    PrimitiveIntIterator nodeRelationshipTypes( long nodeId );

    // SCHEMA RELATED

    DiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId );

    DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId );

    DiffSets<IndexDescriptor> indexChanges();

    DiffSets<IndexDescriptor> constraintIndexChanges();

    Iterable<IndexDescriptor> constraintIndexesCreatedInTx();

    DiffSets<UniquenessConstraint> constraintsChanges();

    DiffSets<UniquenessConstraint> constraintsChangesForLabel( int labelId );

    DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( int labelId,
                                                                          int propertyKey );

    void indexRuleDoAdd( IndexDescriptor descriptor );

    void constraintIndexRuleDoAdd( IndexDescriptor descriptor );

    void indexDoDrop( IndexDescriptor descriptor );

    void constraintIndexDoDrop( IndexDescriptor descriptor );

    void constraintDoAdd( UniquenessConstraint constraint, long indexId );

    void constraintDoDrop( UniquenessConstraint constraint );

    boolean constraintDoUnRemove( UniquenessConstraint constraint );

    boolean constraintIndexDoUnRemove( IndexDescriptor index );

    Long indexCreatedForConstraint( UniquenessConstraint constraint );

    DiffSets<Long> indexUpdates( IndexDescriptor index, Object value );

    void indexUpdateProperty( IndexDescriptor descriptor, long nodeId, DefinedProperty before, DefinedProperty after );
}
