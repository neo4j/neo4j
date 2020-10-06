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
package org.neo4j.kernel.impl.api;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
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
            throw accessMode.onViolation( format( "Schema operation '%s' is not allowed for %s.", action, securityContext.description() ) );
        }
    }

    @Override
    public IndexProviderDescriptor indexProviderByName( String providerName )
    {
        return inner.indexProviderByName( providerName );
    }

    @Override
    public IndexDescriptor indexCreate( IndexPrototype prototype ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( prototype );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor schema, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( schema, name );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor schema, IndexConfig indexConfig, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( schema, indexConfig, name );
    }

    @Override
    public IndexDescriptor indexCreate( SchemaDescriptor schema, String provider, IndexConfig indexConfig, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_INDEX );
        return inner.indexCreate( schema, provider, indexConfig, name );
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
    public void indexDrop( String indexName ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_INDEX );
        inner.indexDrop( indexName );
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate( IndexPrototype prototype ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.uniquePropertyConstraintCreate( prototype );
    }

    @Override
    public ConstraintDescriptor nodeKeyConstraintCreate( IndexPrototype prototype ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.nodeKeyConstraintCreate( prototype );
    }

    @Override
    public ConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor schema, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.nodePropertyExistenceConstraintCreate( schema, name );
    }

    @Override
    public ConstraintDescriptor relationshipPropertyExistenceConstraintCreate( RelationTypeSchemaDescriptor schema, String name ) throws KernelException
    {
        assertSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT );
        return inner.relationshipPropertyExistenceConstraintCreate( schema, name );
    }

    @Override
    public void constraintDrop( SchemaDescriptor schema, ConstraintType type ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_CONSTRAINT );
        inner.constraintDrop( schema, type );
    }

    @Override
    public void constraintDrop( String name ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_CONSTRAINT );
        inner.constraintDrop( name );
    }

    @Override
    public void constraintDrop( ConstraintDescriptor constraint ) throws SchemaKernelException
    {
        assertSchemaWrites( PrivilegeAction.DROP_CONSTRAINT );
        inner.constraintDrop( constraint );
    }
}
