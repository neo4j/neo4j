/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;

abstract class PropertyConstraintDefinition implements ConstraintDefinition
{
    protected final InternalSchemaActions actions;
    protected final String propertyKey;

    protected PropertyConstraintDefinition( InternalSchemaActions actions, String propertyKey )
    {
        this.actions = requireNonNull( actions );
        this.propertyKey = requireNonNull( propertyKey );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        assertInUnterminatedTransaction();
        return singleton( propertyKey );
    }

    @Override
    public boolean isConstraintType( ConstraintType type )
    {
        assertInUnterminatedTransaction();
        return getConstraintType().equals( type );
    }

    @Override
    public abstract boolean equals( Object o );

    @Override
    public abstract int hashCode();

    /**
     * Returned string is used in shell's constraint listing.
     */
    @Override
    public abstract String toString();

    protected void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }
}
