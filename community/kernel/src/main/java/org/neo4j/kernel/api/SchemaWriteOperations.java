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
package org.neo4j.kernel.api;

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
import org.neo4j.kernel.api.schema_new.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;

public interface SchemaWriteOperations
{
    /**
     * Creates an index, indexing properties with the given {@code propertyKeyId} for nodes with the given
     * {@code labelId}.
     * @param schemaDescriptor
     */
    NewIndexDescriptor indexCreate( LabelSchemaDescriptor schemaDescriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException, RepeatedPropertyInCompositeSchemaException;

    /** Drops a {@link NewIndexDescriptor} from the database */
    void indexDrop( NewIndexDescriptor descriptor ) throws DropIndexFailureException;

    /**
     * This should not be used, it is exposed to allow an external job to clean up constraint indexes.
     * That external job should become an internal job, at which point this operation should go away.
     */
    void uniqueIndexDrop( NewIndexDescriptor descriptor ) throws DropIndexFailureException;

    NodeKeyConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException;

    UniquenessConstraintDescriptor uniquePropertyConstraintCreate( LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException;

    NodeExistenceConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException,
            RepeatedPropertyInCompositeSchemaException;

    RelExistenceConstraintDescriptor relationshipPropertyExistenceConstraintCreate(
            RelationTypeSchemaDescriptor relationshipPropertyDescriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException,
            RepeatedPropertyInCompositeSchemaException;

    void constraintDrop( ConstraintDescriptor constraint ) throws DropConstraintFailureException;
}
