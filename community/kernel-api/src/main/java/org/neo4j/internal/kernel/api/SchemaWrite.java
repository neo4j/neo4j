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
import org.neo4j.internal.schema.ConstraintType;
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
     * @param schema description of the index
     * @param name name of the index, or {@code null} to get a generated index name
     * @return the newly created index
     */
    IndexDescriptor indexCreate( SchemaDescriptor schema, String name ) throws KernelException;

    /**
     * Create index from schema descriptor
     *
     * @param schema description of the index
     * @param provider name of the desired index provider implementation, never {@code null}
     * @param name name of the index, or {@code null} to get a generated index name
     * @return the newly created index
     */
    IndexDescriptor indexCreate( SchemaDescriptor schema, String provider, String name ) throws KernelException;

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
     * Drop the index by the given name.
     * @param indexName the name of the index to drop.
     */
    void indexDrop( String indexName ) throws SchemaKernelException;

    /**
     * Create unique property constraint
     *
     * @param schema description of the constraint
     */
    ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor schema, String name ) throws KernelException;

    /**
     * Create unique property constraint
     *
     * @param schema description of the constraint
     * @param provider name of the desired index provider implementation
     */
    ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor schema, String provider, String name ) throws KernelException;

    /**
     * Create node key constraint
     *
     * @param schema description of the constraint
     */
    ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor schema, String name ) throws KernelException;

    /**
     * Create node key constraint
     *
     * @param schema description of the constraint
     * @param provider name of the desired index provider implementation
     */
    ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor schema, String provider, String name ) throws KernelException;

    /**
     * Create node property existence constraint
     *
     * @param schema description of the constraint
     */
    ConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor schema, String name ) throws KernelException;

    /**
     * Create relationship property existence constraint
     *
     * @param schema description of the constraint
     */
    ConstraintDescriptor relationshipPropertyExistenceConstraintCreate( RelationTypeSchemaDescriptor schema, String name ) throws KernelException;

    /**
     * Drop a constraint with the given schema.
     *
     * @param schema The schema of the constraint to be dropped.
     */
    void constraintDrop( SchemaDescriptor schema, ConstraintType type ) throws SchemaKernelException;

    /**
     * Drop the constraint with the given name.
     *
     * @param name The name of the constraint to be dropped.
     */
    void constraintDrop( String name ) throws SchemaKernelException;

    /**
     * Drop the specific constraint.
     *
     * @param constraint description of the constraint
     */
    void constraintDrop( ConstraintDescriptor constraint ) throws SchemaKernelException;
}
