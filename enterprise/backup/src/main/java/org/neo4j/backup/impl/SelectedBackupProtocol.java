/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
