/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
