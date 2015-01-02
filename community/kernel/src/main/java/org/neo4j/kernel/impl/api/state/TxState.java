/**
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
package org.neo4j.kernel.impl.api.state;

import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.util.DiffSets;
import org.neo4j.kernel.api.index.IndexDescriptor;

/**
 * Kernel transaction state, please see {@link TxStateImpl} for details.
 *
 * The naming of methods in this class roughly follows the naming of {@link org.neo4j.kernel.api.Statement}
 * with one exception: All transaction state mutators must include the particle "Do" in their name, e.g.
 * nodeDoAdd. This helps deciding where to set "hasChanges" in the main implementation class {@link TxStateImpl}.
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
        void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed );

        void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitAddedConstraint( UniquenessConstraint element );

        void visitRemovedConstraint( UniquenessConstraint element );
    }

    public abstract boolean hasChanges();

    public abstract void accept( Visitor visitor );


    // ENTITY RELATED

    public abstract Iterable<NodeState> nodeStates();

    public abstract DiffSets<Long> labelStateNodeDiffSets( int labelId );

    public abstract DiffSets<Integer> nodeStateLabelDiffSets( long nodeId );

    public abstract DiffSets<DefinedProperty> nodePropertyDiffSets( long nodeId );

    public abstract DiffSets<DefinedProperty> relationshipPropertyDiffSets( long relationshipId );

    public abstract DiffSets<DefinedProperty> graphPropertyDiffSets();

    /** Returns all nodes that, in this tx, have had labelId added. */
    public abstract Set<Long> nodesWithLabelAdded( int labelId );

    /** Returns all nodes that, in this tx, have had labelId removed.  */
    public abstract DiffSets<Long> nodesWithLabelChanged( int labelId );

    // Temporary: Should become DiffSets<Long> of all node changes, not just deletions
    public abstract DiffSets<Long> nodesDeletedInTx();

    public abstract boolean nodeIsAddedInThisTx( long nodeId );

    public abstract boolean nodeIsDeletedInThisTx( long nodeId );

    public abstract DiffSets<Long> nodesWithChangedProperty( int propertyKeyId, Object value );

    public abstract Map<Long, Object> nodesWithChangedProperty( int propertyKeyId );

    public abstract boolean relationshipIsAddedInThisTx( long relationshipId );

    public abstract boolean relationshipIsDeletedInThisTx( long relationshipId );

    public abstract UpdateTriState labelState( long nodeId, int labelId );

    public abstract void relationshipDoDelete( long relationshipId );

    public abstract void nodeDoDelete( long nodeId );

    public abstract void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty );

    public abstract void relationshipDoReplaceProperty( long relationshipId,
                                                        Property replacedProperty, DefinedProperty newProperty );

    public abstract void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty );

    public abstract void nodeDoRemoveProperty( long nodeId, Property removedProperty );

    public abstract void relationshipDoRemoveProperty( long relationshipId, Property removedProperty );

    public abstract void graphDoRemoveProperty( Property removedProperty );

    public abstract void nodeDoAddLabel( int labelId, long nodeId );

    public abstract void nodeDoRemoveLabel( int labelId, long nodeId );



    // SCHEMA RELATED

    public abstract DiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId );

    public abstract DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId );

    public abstract DiffSets<IndexDescriptor> indexChanges();

    public abstract DiffSets<IndexDescriptor> constraintIndexChanges();

    public abstract Iterable<IndexDescriptor> constraintIndexesCreatedInTx();

    public abstract DiffSets<UniquenessConstraint> constraintsChanges();

    public abstract DiffSets<UniquenessConstraint> constraintsChangesForLabel( int labelId );

    public abstract DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( int labelId,
                                                                                          int propertyKey );

    public abstract void indexRuleDoAdd( IndexDescriptor descriptor );

    public abstract void constraintIndexRuleDoAdd( IndexDescriptor descriptor );

    public abstract void indexDoDrop( IndexDescriptor descriptor );

    public abstract void constraintIndexDoDrop( IndexDescriptor descriptor );

    public abstract void constraintDoAdd( UniquenessConstraint constraint, long indexId );

    public abstract void constraintDoDrop( UniquenessConstraint constraint );

    public abstract boolean constraintDoUnRemove( UniquenessConstraint constraint );

    boolean constraintIndexDoUnRemove( IndexDescriptor index );

    Long indexCreatedForConstraint( UniquenessConstraint constraint );
}
