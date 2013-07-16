/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Set;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public interface TxState
{
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
        void visitNodeLabelChanges( long id, Set<Long> added, Set<Long> removed );

        void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitAddedConstraint( UniquenessConstraint element, long indexId );

        void visitRemovedConstraint( UniquenessConstraint element );
    }
    
    public abstract void accept( Visitor visitor );

    public abstract boolean hasChanges();

    public abstract Iterable<NodeState> nodeStates();

    public abstract DiffSets<Long> labelStateNodeDiffSets( long labelId );

    public abstract DiffSets<Long> nodeStateLabelDiffSets( long nodeId );

    public abstract DiffSets<Property> nodePropertyDiffSets( long nodeId );

    public abstract DiffSets<Property> relationshipPropertyDiffSets( long relationshipId );

    public abstract DiffSets<Property> graphPropertyDiffSets();

    public abstract boolean nodeIsAddedInThisTx( long nodeId );

    public abstract boolean relationshipIsAddedInThisTx( long relationshipId );

    public abstract void nodeDelete( long nodeId );

    public abstract boolean nodeIsDeletedInThisTx( long nodeId );

    public abstract void relationshipDelete( long relationshipId );

    public abstract boolean relationshipIsDeletedInThisTx( long relationshipId );

    public abstract void nodeReplaceProperty( long nodeId, Property replacedProperty, Property newProperty )
            throws PropertyNotFoundException, EntityNotFoundException;

    public abstract void relationshipReplaceProperty( long relationshipId, Property replacedProperty,
            Property newProperty ) throws PropertyNotFoundException, EntityNotFoundException;

    public abstract void graphReplaceProperty( Property replacedProperty, Property newProperty )
            throws PropertyNotFoundException;

    public abstract void nodeRemoveProperty( long nodeId, Property removedProperty ) throws PropertyNotFoundException,
            EntityNotFoundException;

    public abstract void relationshipRemoveProperty( long relationshipId, Property removedProperty )
            throws PropertyNotFoundException, EntityNotFoundException;

    public abstract void graphRemoveProperty( Property removedProperty ) throws PropertyNotFoundException;

    public abstract void nodeAddLabel( long labelId, long nodeId );

    public abstract void nodeRemoveLabel( long labelId, long nodeId );

    /**
     * @return {@code true} if it has been added in this transaction.
     *         {@code false} if it has been removed in this transaction.
     *         {@code null} if it has not been touched in this transaction.
     */
    public abstract Boolean labelState( long nodeId, long labelId );

    /**
     * Returns all nodes that, in this tx, has got labelId added.
     */
    public abstract Set<Long> nodesWithLabelAdded( long labelId );

    /**
     * Returns all nodes that, in this tx, has got labelId removed.
     */
    public abstract DiffSets<Long> nodesWithLabelChanged( long labelId );

    public abstract void addIndexRule( IndexDescriptor descriptor );

    public abstract void addConstraintIndexRule( IndexDescriptor descriptor );

    public abstract void dropIndex( IndexDescriptor descriptor );

    public abstract void dropConstraintIndex( IndexDescriptor descriptor );

    public abstract DiffSets<IndexDescriptor> indexDiffSetsByLabel( long labelId );

    public abstract DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( long labelId );

    public abstract DiffSets<IndexDescriptor> indexChanges();

    public abstract DiffSets<IndexDescriptor> constraintIndexChanges();

    public abstract DiffSets<Long> nodesWithChangedProperty( long propertyKeyId, Object value );

    // Temporary: Should become DiffSets<Long> of all node changes, not just deletions
    public abstract DiffSets<Long> deletedNodes();

    public abstract void addConstraint( UniquenessConstraint constraint, long indexId );

    public abstract DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( long labelId, long propertyKey );

    public abstract DiffSets<UniquenessConstraint> constraintsChangesForLabel( long labelId );

    public abstract DiffSets<UniquenessConstraint> constraintsChanges();

    public abstract void dropConstraint( UniquenessConstraint constraint );

    public abstract boolean unRemoveConstraint( UniquenessConstraint constraint );

    public abstract Iterable<IndexDescriptor> createdConstraintIndexes();
}
