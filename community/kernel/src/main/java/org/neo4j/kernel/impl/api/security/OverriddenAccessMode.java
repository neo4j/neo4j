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
 * Access mode that overrides the original access mode with the overriding mode. Allows exactly what the overriding
 * mode allows, while retaining the meta data of the original mode only.
 */
public class OverriddenAccessMode extends WrappedAccessMode
{
    public OverriddenAccessMode( AccessMode original, AccessMode overriding )
    {
        super( original, overriding );
    }

    @Override
    public boolean allowsWrites()
    {
        return wrapping.allowsWrites();
    }

    @Override
    public boolean allowsTokenCreates( PrivilegeAction action )
    {
        return wrapping.allowsTokenCreates( action );
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return wrapping.allowsSchemaWrites();
    }

    @Override
    public boolean allowsSchemaWrites( PrivilegeAction action )
    {
        return wrapping.allowsSchemaWrites( action );
    }

    @Override
    public boolean allowsTraverseAllLabels()
    {
        return wrapping.allowsTraverseAllLabels();
    }

    @Override
    public boolean allowsTraverseAllNodesWithLabel( long label )
    {
        return wrapping.allowsTraverseAllNodesWithLabel( label );
    }

    @Override
    public boolean disallowsTraverseLabel( long label )
    {
        return wrapping.disallowsTraverseLabel( label );
    }

    @Override
    public boolean allowsTraverseNode( long... labels )
    {
        return wrapping.allowsTraverseNode( labels );
    }

    @Override
    public boolean allowsTraverseAllRelTypes()
    {
        return wrapping.allowsTraverseAllRelTypes();
    }

    @Override
    public boolean allowsTraverseRelType( int relType )
    {
        return wrapping.allowsTraverseRelType( relType );
    }

    @Override
    public boolean allowsReadPropertyAllLabels( int propertyKey )
    {
        return wrapping.allowsReadPropertyAllLabels( propertyKey );
    }

    @Override
    public boolean disallowsReadPropertyForSomeLabel( int propertyKey )
    {
        return wrapping.disallowsReadPropertyForSomeLabel( propertyKey );
    }

    @Override
    public boolean allowsReadNodeProperty( Supplier<LabelSet> labels, int propertyKey )
    {
        return wrapping.allowsReadNodeProperty( labels, propertyKey );
    }

    @Override
    public boolean allowsReadPropertyAllRelTypes( int propertyKey )
    {
        return wrapping.allowsReadPropertyAllRelTypes( propertyKey );
    }

    @Override
    public boolean allowsReadRelationshipProperty( IntSupplier relType, int propertyKey )
    {
        return wrapping.allowsReadRelationshipProperty( relType, propertyKey );
    }

    @Override
    public boolean allowsSeePropertyKeyToken( int propertyKey )
    {
        return wrapping.allowsSeePropertyKeyToken( propertyKey );
    }

    @Override
    public boolean allowsProcedureWith( String[] allowed )
    {
        return false;
    }

    @Override
    public String name()
    {
        return original.name() + " overridden by " + wrapping.name();
    }
}
