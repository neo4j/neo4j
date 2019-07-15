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
package org.neo4j.internal.schema;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Represents a stored schema rule.
 */
public interface SchemaRule extends SchemaDescriptorSupplier
{
    static String nameOrDefault( String name, String defaultName )
    {
        // Because of the difference how blank and trim works (whitespace check is totally different in those) we can't simply use isBlank here
        // and need to double check invoking trim that as result we do not have empty string there
        if ( isBlank( name ) || name.trim().isEmpty() )
        {
            name = defaultName;
        }
        return sanitiseName( name );
    }

    static String sanitiseName( String name )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be null." );
        }
        name = name.trim();
        if ( name.isEmpty() )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be the empty string." );
        }
        else
        {
            int length = name.length();
            for ( int i = 0; i < length; i++ )
            {
                char ch = name.charAt( i );
                if ( ch == '\0' )
                {
                    throw new IllegalArgumentException( "Schema rule names are not allowed to contain null-bytes: '" + name + "'." );
                }
            }
        }
        if ( ReservedSchemaRuleNames.contains( name ) )
        {
            throw new IllegalArgumentException( "The index name '" + name + "' is reserved, and cannot be used. " +
                    "The reserved names are " + ReservedSchemaRuleNames.getReservedNames() + "." );
        }
        return name;
    }

    /**
     * The persistence id for this rule.
     */
    long getId();

    /**
     * @return The (possibly user supplied) name of this schema rule.
     */
    String getName();
}
