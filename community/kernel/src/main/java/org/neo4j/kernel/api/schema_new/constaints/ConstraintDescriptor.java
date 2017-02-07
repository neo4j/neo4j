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
package org.neo4j.kernel.api.schema_new.constaints;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;

import static java.lang.String.format;

/**
 * Internal representation of a graph constraint, including the schema unit it targets (eg. label-property combination)
 * and the how that schema unit is constrained (eg. "has to exist", or "must be unique").
 */
public class ConstraintDescriptor implements SchemaDescriptor.Supplier
{
    public enum Type { UNIQUE, EXISTS }

    public interface Supplier
    {
        ConstraintDescriptor getConstraintDescriptor();
    }

    private final SchemaDescriptor schema;
    private final ConstraintDescriptor.Type type;

    ConstraintDescriptor( SchemaDescriptor schema, Type type )
    {
        this.schema = schema;
        this.type = type;
    }

    // METHODS

    @Override
    public SchemaDescriptor schema()
    {
        return schema;
    }

    public Type type()
    {
        return type;
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */

    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( "Constraint( %s, %s )", type.name(), schema.userDescription( tokenNameLookup ) );
    }

    /**
     * Checks whether a constraint descriptor Supplier supplies this constraint descriptor.
     * @param supplier supplier to get a constraint descriptor from
     * @return true if the supplied constraint descriptor equals this constraint descriptor
     */
    public boolean isSame( Supplier supplier )
    {
        return this.equals( supplier.getConstraintDescriptor() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor that = (ConstraintDescriptor)o;
            return this.type() == that.type() && this.schema().equals( that.schema() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode() & schema.hashCode();
    }
}
