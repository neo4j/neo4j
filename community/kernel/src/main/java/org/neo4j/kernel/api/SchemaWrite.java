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

import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.ProcedureConstraintViolation;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.procedures.ProcedureSignature;
import org.neo4j.kernel.api.procedures.ProcedureSignature.ProcedureName;

interface SchemaWrite
{
    /**
     * Creates an index, indexing properties with the given {@code propertyKeyId} for nodes with the given
     * {@code labelId}.
     */
    IndexDescriptor indexCreate( int labelId, int propertyKeyId )
            throws AlreadyIndexedException, AlreadyConstrainedException;

    /** Drops a {@link IndexDescriptor} from the database */
    void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException;

    UniquenessConstraint uniquePropertyConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException;

    NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException;

    RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate( int relationshipTypeId,
            int propertyKeyId ) throws CreateConstraintFailureException, AlreadyConstrainedException;

    void constraintDrop( NodePropertyConstraint constraint ) throws DropConstraintFailureException;

    void constraintDrop( RelationshipPropertyConstraint constraint ) throws DropConstraintFailureException;

    /**
     * This should not be used, it is exposed to allow an external job to clean up constraint indexes.
     * That external job should become an internal job, at which point this operation should go away.
     */
    void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException;

    /**
     * @param signature the namespace, name, typed inputs and typed outputs
     * @param language named procedure language, eg "js"
     * @param body the procedure code, format is language-handler specific
     */
    void procedureCreate( ProcedureSignature signature, String language, String body ) throws ProcedureException, ProcedureConstraintViolation;

    /**
     * Drop a procedure from the database.
     * @param name
     */
    void procedureDrop( ProcedureName name ) throws ProcedureConstraintViolation, ProcedureException;
}
