/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.schema.constaints;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema.SchemaDescriptor;

import static java.lang.String.format;

/**
 * Internal representation of a graph constraint, including the schema unit it targets (eg. label-property combination)
 * and the how that schema unit is constrained (eg. "has to exist", or "must be unique").
 */
public abstract class ConstraintDescriptor implements SchemaDescriptor.Supplier
{
    public enum Type
    {
        UNIQUE( true, false ),
        EXISTS( false, true ),
        UNIQUE_EXISTS( true, true );

        private final boolean isUnique;
        private final boolean mustExist;

        Type( boolean isUnique, boolean mustExist )
        {
            this.isUnique = isUnique;
            this.mustExist = mustExist;
        }

        private boolean enforcesUniqueness()
        {
            return isUnique;
        }

        private boolean enforcesPropertyExistence()
        {
            return mustExist;
        }
    }

    public interface Supplier
    {
        ConstraintDescriptor getConstraintDescriptor();
    }

    private final ConstraintDescriptor.Type type;

    ConstraintDescriptor( Type type )
    {
        this.type = type;
    }

    // METHODS

    @Override
    public abstract SchemaDescriptor schema();

    public Type type()
    {
        return type;
    }

    public boolean enforcesUniqueness()
    {
        return type.enforcesUniqueness();
    }

    public boolean enforcesPropertyExistence()
    {
        return type.enforcesPropertyExistence();
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of this constraint.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( "Constraint( %s, %s )", type.name(), schema().userDescription( tokenNameLookup ) );
    }

    /**
     * Checks whether a constraint descriptor Supplier supplies this constraint descriptor.
     * @param supplier supplier to get a constraint descriptor from
     * @return true if the supplied constraint descriptor equals this constraint descriptor
     */
    public final boolean isSame( Supplier supplier )
    {
        return this.equals( supplier.getConstraintDescriptor() );
    }

    @Override
    public final boolean equals( Object o )
    {
        if ( o != null && o instanceof ConstraintDescriptor )
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

    // PRETTY PRINTING

    public abstract String prettyPrint( TokenNameLookup tokenNameLookup );

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
