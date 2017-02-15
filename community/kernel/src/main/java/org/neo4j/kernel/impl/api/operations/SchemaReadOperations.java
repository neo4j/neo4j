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
package org.neo4j.kernel.impl.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.storageengine.api.schema.PopulationProgress;

public interface SchemaReadOperations
{
    /**
     * Returns the descriptor for the given labelId and propertyKey.
     */
    NewIndexDescriptor indexGetForLabelAndPropertyKey( KernelStatement state, NodePropertyDescriptor descriptor );

    /**
     * Get all indexes for a label.
     */
    Iterator<NewIndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId );

    /**
     * Returns all indexes.
     */
    Iterator<NewIndexDescriptor> indexesGetAll( KernelStatement state );

    /**
     * Get all constraint indexes for a label.
     */
    Iterator<NewIndexDescriptor> uniqueIndexesGetForLabel( KernelStatement state, int labelId );

    /**
     * Returns all constraint indexes.
     */
    Iterator<NewIndexDescriptor> uniqueIndexesGetAll( KernelStatement state );

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState indexGetState( KernelStatement state, NewIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Retrieve the population progress of an index.
     */
    PopulationProgress indexGetPopulationProgress( KernelStatement state, NewIndexDescriptor descriptor ) throws
            IndexNotFoundKernelException;

    /**
     * Get the index size.
     **/
    long indexSize( KernelStatement state, NewIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Calculate the index unique values percentage.
     **/
    double indexUniqueValuesPercentage( KernelStatement state, NewIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns the failure description of a failed index.
     */
    String indexGetFailure( Statement state, NewIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKeys. There are only {@link NodePropertyConstraint}
     * for the time being.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( KernelStatement state,
            NodePropertyDescriptor descriptor );

    /**
     * Get all constraints applicable to label. There are only {@link NodePropertyConstraint}
     * for the time being.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabel( KernelStatement state, int labelId );

    /**
     * Get all constraints applicable to relationship type and propertyKey.
     * There are only {@link RelationshipPropertyConstraint} for the time being.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( KernelStatement state,
            RelationshipPropertyDescriptor descriptor );

    /**
     * Get all constraints applicable to relationship type. There are only {@link RelationshipPropertyConstraint}
     * for the time being.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( KernelStatement state, int typeId );

    /**
     * Get all constraints. There are only {@link PropertyConstraint}
     * for the time being.
     */
    Iterator<PropertyConstraint> constraintsGetAll( KernelStatement state );

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( KernelStatement state, NewIndexDescriptor index ) throws SchemaRuleNotFoundException;

    /**
     * Get the index id (the id or the schema rule record) for a committed index
     * - throws exception for indexes that aren't committed.
     */
    long indexGetCommittedId( KernelStatement state, NewIndexDescriptor index ) throws SchemaRuleNotFoundException;
}
