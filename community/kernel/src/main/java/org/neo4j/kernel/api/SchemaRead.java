/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateIndexSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.procedures.ProcedureDescriptor;
import org.neo4j.kernel.api.procedures.ProcedureSignature;
import org.neo4j.kernel.api.procedures.ProcedureSignature.ProcedureName;

interface SchemaRead
{
    /** Returns the index rule for the given labelId and propertyKey. */
    IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKey )
            throws SchemaRuleNotFoundException;

    /** Get all indexes for a label. */
    Iterator<IndexDescriptor> indexesGetForLabel( int labelId );

    /** Returns all indexes. */
    Iterator<IndexDescriptor> indexesGetAll();

    /** Returns the constraint index for the given labelId and propertyKey. */
    IndexDescriptor uniqueIndexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateIndexSchemaRuleException;

    /** Get all constraint indexes for a label. */
    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId );

    /** Returns all constraint indexes. */
    Iterator<IndexDescriptor> uniqueIndexesGetAll();

    /** Retrieve the state of an index. */
    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Get the index size. */
    long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Calculate the index unique values percentage (range: {@code 0.0} exclusive to {@code 1.0} inclusive). */
    double indexUniqueValuesSelectivity( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Returns the failure description of a failed index. */
    String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKey. There are only {@link NodePropertyConstraint}
     * for the time being.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId );

    /**
     * Get all constraints applicable to label. There are only {@link NodePropertyConstraint}
     * for the time being.
     */
    Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId );

    /**
     * Get all constraints applicable to relationship type. There are only {@link RelationshipPropertyConstraint}
     * for the time being.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId );

    /**
     * Get all constraints applicable to relationship type and propertyKey.
     * There are only {@link RelationshipPropertyConstraint} for the time being.
     */
    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId, int propertyKeyId );

    /**
     * Get all constraints. There are only {@link PropertyConstraint}
     * for the time being.
     */
    Iterator<PropertyConstraint> constraintsGetAll();

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException;

    /** Get all procedures defined in the system */
    Iterator<ProcedureSignature> proceduresGetAll();

    /** Fetch a procedure given its signature. */
    ProcedureDescriptor procedureGet( ProcedureName name ) throws ProcedureException;
}
