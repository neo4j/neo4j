/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;

/**
 * Surface for creating and dropping indexes and constraints.
 */
public interface SchemaWrite
{
    IndexDescriptor indexCreate( IndexPrototype prototype ) throws KernelException;

    /**
     * Create index from schema descriptor
     *
     * @param descriptor description of the index
     * @return the newly created index
     */
    IndexDescriptor indexCreate( SchemaDescriptor descriptor ) throws KernelException;

    /**
     * Create index from schema descriptor
     *
     * @param descriptor description of the index
     * @param name name of the index
     * @return the newly created index
     */
    IndexDescriptor indexCreate( SchemaDescriptor descriptor, String name ) throws KernelException;

    /**
     * Create index from schema descriptor
     *
     * @param descriptor description of the index
     * @param provider name of the desired index provider implementation
     * @param name name of the index
     * @return the newly created index
     */
    IndexDescriptor indexCreate( SchemaDescriptor descriptor, String provider, String name ) throws KernelException;

    /**
     * Drop the given index
     *
     * @param index the index to drop
     */
    void indexDrop( IndexDescriptor index ) throws SchemaKernelException;

    /**
     * Drop an index that matches the given schema.
     *
     * @param schema the schema matching the index to drop
     */
    void indexDrop( SchemaDescriptor schema ) throws SchemaKernelException;

    /**
     * Create unique property constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor, String name ) throws KernelException;

    /**
     * Create unique property constraint
     *
     * @param descriptor description of the constraint
     * @param provider name of the desired index provider implementation
     */
    ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor, String provider, String name ) throws KernelException;

    /**
     * Create node key constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor, String name ) throws KernelException;

    /**
     * Create node key constraint
     *
     * @param descriptor description of the constraint
     * @param provider name of the desired index provider implementation
     */
    ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor, String provider, String name ) throws KernelException;

    /**
     * Create node property existence constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor descriptor, String name ) throws KernelException;

    /**
     * Create relationship property existence constraint
     *
     * @param descriptor description of the constraint
     */
    ConstraintDescriptor relationshipPropertyExistenceConstraintCreate( RelationTypeSchemaDescriptor descriptor, String name ) throws KernelException;

    /**
     * Drop a constraint with the given schema.
     *
     * @param schema The schema of the constraint to be dropped.
     */
    void constraintDrop( SchemaDescriptor schema ) throws SchemaKernelException;

    /**
     * Drop the specific constraint.
     *
     * @param descriptor description of the constraint
     */
    void constraintDrop( ConstraintDescriptor descriptor ) throws SchemaKernelException;
}
