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
     * Create index from schema descriptor. Default configured index provider will be used.
     *
     * @param descriptor description of the index
     */
    default IndexReference indexCreate( SchemaDescriptor descriptor ) throws SchemaKernelException
    {
        return indexCreate( descriptor, null );
    }

    /**
     * Create index from schema descriptor. Optionally a specific provider name can be specified.
     *
     * @param descriptor description of the index
     * @param providerName specific index provider this index will be created for. If {@code null} then the default configured will be used.
     * @return the newly created index
     */
    IndexReference indexCreate( SchemaDescriptor descriptor, String providerName ) throws SchemaKernelException;

    /**
     * Drop the given index
     *
     * @param index the index to drop
     */
    void indexDrop( IndexReference index ) throws SchemaKernelException;

    /**
     * Create unique property constraint. Default configured index provider will be used.
     *
     * @param descriptor description of the constraint
     */
    default ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor ) throws SchemaKernelException
    {
        return uniquePropertyConstraintCreate( descriptor, null );
    }

    /**
     * Create unique property constraint. Optionally a specific provider name can be specified.
     *
     * @param descriptor description of the constraint
     * @param providerName specific index provider this index will be created for. If {@code null} then the default configured will be used.
     * @return the newly created index
     */
    ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor, String providerName ) throws SchemaKernelException;

    /**
     * Create node key constraint. Default configured index provider will be used.
     *
     * @param descriptor description of the constraint
     */
    default ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor ) throws SchemaKernelException
    {
        return nodeKeyConstraintCreate( descriptor, null );
    }

    /**
     * Create node key constraint. Optionally a specific provider name can be specified.
     *
     * @param descriptor description of the constraint
     * @param providerName specific index provider this index will be created for. If {@code null} then the default configured will be used.
     * @return the newly created index
     */
    ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor, String providerName ) throws SchemaKernelException;

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
