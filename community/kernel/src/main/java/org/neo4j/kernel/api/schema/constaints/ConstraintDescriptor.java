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
package org.neo4j.kernel.api.schema.constaints;

import org.neo4j.internal.kernel.api.TokenNameLookup;

import static java.lang.String.format;

/**
 * Internal representation of a graph constraint, including the schema unit it targets (eg. label-property combination)
 * and the how that schema unit is constrained (eg. "has to exist", or "must be unique").
 */
public abstract class ConstraintDescriptor implements org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor
{

    private final ConstraintDescriptor.Type type;

    ConstraintDescriptor( Type type )
    {
        this.type = type;
    }

    // METHODS

    @Override
    public Type type()
    {
        return type;
    }

    @Override
    public boolean enforcesUniqueness()
    {
        return type.enforcesUniqueness();
    }

    @Override
    public boolean enforcesPropertyExistence()
    {
        return type.enforcesPropertyExistence();
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of this constraint.
     */
    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( "Constraint( %s, %s )", type.name(), schema().userDescription( tokenNameLookup ) );
    }

    @Override
    public final boolean isSame( Supplier supplier )
    {
        return this.equals( supplier.getConstraintDescriptor() );
    }

    @Override
    public final boolean equals( Object o )
    {
        if ( o instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor that = (ConstraintDescriptor)o;
            return this.type() == that.type() && this.schema().equals( that.schema() );
        }
        return false;
    }

    @Override
    public final int hashCode()
    {
        return type.hashCode() & schema().hashCode();
    }

    String escapeLabelOrRelTyp( String name )
    {
        if ( name.contains( ":" ) )
        {
            return "`" + name + "`";
        }
        else
        {
            return name;
        }
    }
}
