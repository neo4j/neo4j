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

import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;

public interface SchemaWriteOperations
{
    /**
     * Creates an index, indexing properties with the given {@code propertyKeyId} for nodes with the given
     * {@code labelId}.
     */
    NewIndexDescriptor indexCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException, RepeatedPropertyInCompositeSchemaException;

    /** Drops a {@link NewIndexDescriptor} from the database */
    void indexDrop( KernelStatement state, NewIndexDescriptor descriptor ) throws DropIndexFailureException;

    /**
     * This should not be used, it is exposed to allow an external job to clean up constraint indexes.
     * That external job should become an internal job, at which point this operation should go away.
     */
    void uniqueIndexDrop( KernelStatement state, NewIndexDescriptor descriptor ) throws DropIndexFailureException;

    UniquenessConstraintDescriptor uniquePropertyConstraintCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException;

    NodeExistenceConstraintDescriptor nodePropertyExistenceConstraintCreate( KernelStatement state,
            LabelSchemaDescriptor descriptor ) throws AlreadyConstrainedException, CreateConstraintFailureException,
            RepeatedPropertyInCompositeSchemaException;

    RelExistenceConstraintDescriptor relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            RelationTypeSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException,
            RepeatedPropertyInCompositeSchemaException;

    void constraintDrop( KernelStatement state, ConstraintDescriptor constraint ) throws DropConstraintFailureException;
}
