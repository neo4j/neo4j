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
package org.neo4j.internal.kernel.api.security;

import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.LabelSet;

public class TestAccessMode implements AccessMode
{
    private final boolean allowRead;
    private final boolean allowReadAll;
    private final boolean allowWrite;
    private final boolean allowSchema;

    public TestAccessMode( boolean allowRead, boolean allowReadAll, boolean allowWrite, boolean allowSchema )
    {
        this.allowRead = allowRead;
        this.allowReadAll = allowReadAll;
        this.allowWrite = allowWrite;
        this.allowSchema = allowSchema;
    }

    @Override
    public boolean allowsWrites()
    {
        return allowWrite;
    }

    @Override
    public boolean allowsTokenCreates( PrivilegeAction action )
    {
        return allowWrite;
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return allowSchema;
    }

    @Override
    public boolean allowsSchemaWrites( PrivilegeAction action )
    {
        return allowSchema;
    }

    @Override
    public boolean allowsTraverseAllLabels()
    {
        return allowReadAll;
    }

    @Override
    public boolean allowsTraverseAllNodesWithLabel( long label )
    {
        return allowReadAll;
    }

    @Override
    public boolean disallowsTraverseLabel( long label )
    {
        return !allowRead;
    }

    @Override
    public boolean allowsTraverseNode( long... labels )
    {
        return allowRead;
    }

    @Override
    public boolean allowsTraverseAllRelTypes()
    {
        return allowReadAll;
    }

    @Override
    public boolean allowsTraverseRelType( int relType )
    {
        return allowRead;
    }

    @Override
    public boolean allowsReadPropertyAllLabels( int propertyKey )
    {
        return allowReadAll;
    }

    @Override
    public boolean disallowsReadPropertyForSomeLabel( int propertyKey )
    {
        return !allowReadAll;
    }

    @Override
    public boolean allowsReadNodeProperty( Supplier<LabelSet> labels, int propertyKey )
    {
        return allowRead;
    }

    @Override
    public boolean allowsReadPropertyAllRelTypes( int propertyKey )
    {
        return allowReadAll;
    }

    @Override
    public boolean allowsReadRelationshipProperty( IntSupplier relType, int propertyKey )
    {
        return allowRead;
    }

    @Override
    public boolean allowsSeePropertyKeyToken( int propertyKey )
    {
        return allowRead;
    }

    @Override
    public boolean allowsProcedureWith( String[] allowed )
    {
        return false;
    }

    @Override
    public AuthorizationViolationException onViolation( String msg )
    {
        return new AuthorizationViolationException( "Forbidden in testAccessMode" );
    }

    @Override
    public String name()
    {
        return "Test";
    }
}
