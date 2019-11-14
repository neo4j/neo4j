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
package org.neo4j.configuration.ssl;

import static org.neo4j.configuration.ssl.ClientAuth.NONE;
import static org.neo4j.configuration.ssl.ClientAuth.OPTIONAL;
import static org.neo4j.configuration.ssl.ClientAuth.REQUIRE;

public enum SslPolicyScope
{
    BOLT( OPTIONAL, "certificates/bolt" ),
    HTTPS( OPTIONAL, "certificates/https" ),
    CLUSTER( REQUIRE, "certificates/cluster" ),
    BACKUP( REQUIRE, "certificates/backup" ),
    FABRIC( NONE, "certificates/fabric", true ),
    TESTING( REQUIRE, "certificates/testing" );

    final ClientAuth authDefault;
    final String baseDir;
    private final boolean clientOnly;

    SslPolicyScope( ClientAuth authDefault, String baseDir )
    {
        this( authDefault, baseDir, false );
    }

    SslPolicyScope( ClientAuth authDefault, String baseDir, boolean clientOnly )
    {
        this.authDefault = authDefault;
        this.baseDir = baseDir;
        this.clientOnly = clientOnly;
    }

    public boolean isClientOnly()
    {
        return clientOnly;
    }

    static SslPolicyScope fromName( String name )
    {
        for ( SslPolicyScope value : values() )
        {
            if ( value.name().equalsIgnoreCase( name ) )
            {
                return value;
            }
        }
        return null;
    }
}
