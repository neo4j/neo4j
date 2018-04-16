/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.impl;

import java.util.stream.Stream;

public enum SelectedBackupProtocol
{
    ANY( "any" ),
    COMMON( "common" ),
    CATCHUP( "catchup" );

    public String getName()
    {
        return name;
    }

    private final String name;

    SelectedBackupProtocol( String name )
    {
        this.name = name;
    }

    public static SelectedBackupProtocol fromUserInput( String value )
    {
        return Stream.of( SelectedBackupProtocol.values() )
                .filter( proto -> value.equals( proto.name ) )
                .findFirst()
                .orElseThrow( () -> new RuntimeException( String.format( "Failed to parse `%s`", value ) ) );
    }
}
