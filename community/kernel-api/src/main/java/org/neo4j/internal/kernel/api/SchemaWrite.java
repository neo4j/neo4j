/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.kernel.api;

import java.util.Optional;

import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;

/**
 * Surface for creating and dropping indexes and constraints.
 */
public interface SchemaWrite
{
    /**
     * Create index from schema descriptor
     *
     * @param descriptor description of the index
     * @return the newly created index
     */
    IndexReference indexCreate( SchemaDescriptor descriptor ) throws SchemaKernelException;

    /**
     * Create index from schema descriptor
     *
     * @param descriptor description of the index
     * @param name name of the index
     * @return the newly created index
     */
    IndexReference indexCreate( SchemaDescriptor descriptor, Optional<String> name ) throws SchemaKernelException;

    /**
     * Create index from schema descriptor
     *
     * @param descriptor description of the index
     * @param provider name of the desired index provider implementation
     * @param name name of the index
     * @return the newly created index
     */
    IndexReference indexCreate( SchemaDescriptor descriptor, String provider, Optional<String> name ) throws SchemaKernelException;

    /**
     * Drop the given index
     *
     * @param index the index to drop
     */
    void indexDrop( IndexReference index ) throws SchemaKernelException;

    /**
     * Create unique property constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor ) throws SchemaKernelException;

    /**
     * Create unique property constraint
     *
     * @param descriptor description of the constraint
     * @param provider name of the desired index provider implementation
     */
    ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor, String provider ) throws SchemaKernelException;

    /**
     * Create node key constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor ) throws SchemaKernelException;

    /**
     * Create node key constraint
     *
     * @param descriptor description of the constraint
     * @param provider name of the desired index provider implementation
     */
    ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor, String provider ) throws SchemaKernelException;

    /**
     * Create node property existence constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor descriptor ) throws SchemaKernelException;

    /**
     * Create relationship property existence constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor relationshipPropertyExistenceConstraintCreate( RelationTypeSchemaDescriptor descriptor )
            throws SchemaKernelException;

    /**
     * Drop constraint
     *
     * @param descriptor description of the constraint
     */
    void constraintDrop( ConstraintDescriptor descriptor ) throws SchemaKernelException;
}
