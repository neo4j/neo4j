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
package org.neo4j.internal.schema;

import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * This enum contains names that {@link SchemaRule#getName()} <em>could</em> perhaps return, but which are reserved for current or future internal use.
 * That is, no user, admin or operator can create schema rules with these names.
 */
public enum ReservedSchemaRuleNames
{
    /**
     * The name used for NO_INDEX equivalents in pre-4.0 versions.
     */
    UNNAMED_INDEX( "Unnamed index" ),
    /**
     * Used by {@link IndexDescriptor#NO_INDEX}.
     */
    NO_INDEX( "<No index>" ),

    NO_CONSTRAINT( "<No constraint>" ), // Reserved for future.
    NO_RULE( "<No rule>" ), // Reserved for future.
    NO_SCHEMA( "<No schema>" ), // Reserved for future.
    NO_TRIGGER( "<No trigger>" ), // Reserved for future.
    NO_SEQUENCE( "<No sequence>" ), // Reserved for future.
    NO_CATALOG( "<No catalog>" ), // Reserved for future.
    NO_DATABASE( "<No database>" ), // Reserved for future.
    NO_GRAPH( "<No graph>" ), // Reserved for future.
    NO_MAP( "<No map>" ), // Reserved for future.
    NO_OBJECT( "<No object>" ); // Reserved for future.

    private static final Set<String> RESERVED_NAMES = Stream.of( values() ).map( ReservedSchemaRuleNames::getReservedName ).collect( toUnmodifiableSet() );
    private final String reservedName;

    ReservedSchemaRuleNames( String reservedName )
    {
        this.reservedName = reservedName;
    }

    public static boolean contains( String name )
    {
        return RESERVED_NAMES.contains( name );
    }

    public static Set<String> getReservedNames()
    {
        return RESERVED_NAMES;
    }

    public String getReservedName()
    {
        return reservedName;
    }
}
