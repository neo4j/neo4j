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
package org.neo4j.kernel.impl.api.security;

import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;

/**
 * Access mode that restricts the original access mode with the restricting mode. Allows things that both the
 * original and the restricting mode allows, while retaining the meta data of the original mode only.
 */
public class RestrictedAccessMode extends WrappedAccessMode
{
    public RestrictedAccessMode( AccessMode original, AccessMode restricting )
    {
        super( original, restricting );
    }

    @Override
    public boolean allowsWrites()
    {
        return original.allowsWrites() && wrapping.allowsWrites();
    }

    @Override
    public boolean allowsTokenCreates( PrivilegeAction action )
    {
        return original.allowsTokenCreates( action ) && wrapping.allowsTokenCreates( action );
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return original.allowsSchemaWrites() && wrapping.allowsSchemaWrites();
    }

    @Override
    public boolean allowsSchemaWrites( PrivilegeAction action )
    {
        return original.allowsSchemaWrites( action ) && wrapping.allowsSchemaWrites( action );
    }

    @Override
    public boolean allowsTraverseAllLabels()
    {
        return original.allowsTraverseAllLabels() && wrapping.allowsTraverseAllLabels();
    }

    @Override
    public boolean allowsTraverseAllNodesWithLabel( long label )
    {
        return original.allowsTraverseAllNodesWithLabel( label ) && wrapping.allowsTraverseAllNodesWithLabel( label );
    }

    @Override
    public boolean disallowsTraverseLabel( long label )
    {
        return original.disallowsTraverseLabel( label ) || wrapping.disallowsTraverseLabel( label );
    }

    @Override
    public boolean allowsTraverseNode( long... labels )
    {
        return original.allowsTraverseNode( labels ) && wrapping.allowsTraverseNode( labels );
    }

    @Override
    public boolean allowsTraverseAllRelTypes()
    {
        return original.allowsTraverseAllRelTypes() && wrapping.allowsTraverseAllRelTypes();
    }

    @Override
    public boolean allowsTraverseRelType( int relType )
    {
        return original.allowsTraverseRelType( relType ) && wrapping.allowsTraverseRelType( relType );
    }

    @Override
    public boolean allowsReadPropertyAllLabels( int propertyKey )
    {
        return original.allowsReadPropertyAllLabels( propertyKey ) && wrapping.allowsReadPropertyAllLabels( propertyKey );
    }

    @Override
    public boolean disallowsReadPropertyForSomeLabel( int propertyKey )
    {
        return original.disallowsReadPropertyForSomeLabel( propertyKey ) && wrapping.disallowsReadPropertyForSomeLabel( propertyKey );
    }

    @Override
    public boolean allowsReadNodeProperty( Supplier<LabelSet> labels, int propertyKey )
    {
        return original.allowsReadNodeProperty( labels, propertyKey ) && wrapping.allowsReadNodeProperty( labels, propertyKey );
    }

    @Override
    public boolean allowsReadPropertyAllRelTypes( int propertyKey )
    {
        return original.allowsReadPropertyAllRelTypes( propertyKey ) && wrapping.allowsReadPropertyAllRelTypes( propertyKey );
    }

    @Override
    public boolean allowsReadRelationshipProperty( IntSupplier relType, int propertyKey )
    {
        return original.allowsReadRelationshipProperty( relType, propertyKey ) && wrapping.allowsReadRelationshipProperty( relType, propertyKey );
    }

    @Override
    public boolean allowsSeePropertyKeyToken( int propertyKey )
    {
        return original.allowsSeePropertyKeyToken( propertyKey ) && wrapping.allowsSeePropertyKeyToken( propertyKey );
    }

    @Override
    public boolean allowsProcedureWith( String[] allowed )
    {
        return false;
    }

    @Override
    public String name()
    {
        return original.name() + " restricted to " + wrapping.name();
    }
}
