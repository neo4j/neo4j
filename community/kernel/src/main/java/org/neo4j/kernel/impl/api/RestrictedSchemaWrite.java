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
package org.neo4j.kernel.impl.api;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;

import static java.lang.String.format;

public class RestrictedSchemaWrite implements SchemaWrite
{
    private SchemaWrite inner;
    private SecurityContext securityContext;

    RestrictedSchemaWrite( SchemaWrite inner, SecurityContext securityContext )
    {
        this.inner = inner;
        this.securityContext = securityContext;
    }

    private void assertSchemaWrites( PrivilegeAction action )
    {
        AccessMode accessMode = securityContext.mode();
        if ( !accessMode.allowsSchemaWrites( action ) )
        {
            throw accessMode.onViolation( format( "Schema operation '%s' is not allowed for %s.", action.toString(), securityContext.description() ) );
        }
    }

    @Override
    public IndexDescriptor indexCreate( IndexPrototype prototype ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( prototype );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor descriptor ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( descriptor );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor descriptor, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( descriptor, name );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor descriptor, String provider, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( descriptor, provider, name );
    }

    @Override
    public void indexDrop( IndexDescriptor index ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_INDEX );
        inner.indexDrop( index );
    }

    @Override
    public void indexDrop( SchemaDescriptor schema ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_INDEX );
        inner.indexDrop( schema );
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.uniquePropertyConstraintCreate( descriptor, name );
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate( SchemaDescriptor descriptor, String provider, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.uniquePropertyConstraintCreate( descriptor, provider, name );
    }

    @Override
    public ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.nodeKeyConstraintCreate( descriptor, name );
    }

    @Override
    public ConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor, String provider, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.nodeKeyConstraintCreate( descriptor, provider, name );
    }

    @Override
    public ConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor descriptor, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.nodePropertyExistenceConstraintCreate( descriptor, name );
    }

    @Override
    public ConstraintDescriptor relationshipPropertyExistenceConstraintCreate( RelationTypeSchemaDescriptor descriptor, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.relationshipPropertyExistenceConstraintCreate( descriptor, name );
    }

    @Override
    public void constraintDrop( SchemaDescriptor schema ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_CONSTRAINT );
        inner.constraintDrop( schema );
    }

    @Override
    public void constraintDrop( ConstraintDescriptor descriptor ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_CONSTRAINT );
        inner.constraintDrop( descriptor );
    }
}
