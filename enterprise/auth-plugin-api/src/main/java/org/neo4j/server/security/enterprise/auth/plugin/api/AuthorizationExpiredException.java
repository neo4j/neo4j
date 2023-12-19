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
package org.neo4j.server.security.enterprise.auth.plugin.api;

import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;

/**
 * An exception that can be thrown if authorization has expired and the user needs to re-authenticate
 * in order to renew authorization.
 * Throwing this exception will cause the server to disconnect the client.
 *
 * <p>This is typically used from the {@link AuthorizationPlugin#authorize}
 * method of a combined authentication and authorization plugin (that implements the two separate interfaces
 * {@link AuthenticationPlugin} and {@link AuthorizationPlugin}), that manages its own caching of auth info.
 *
 * @see org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin
 */
public class AuthorizationExpiredException extends RuntimeException
{
    public AuthorizationExpiredException( String message )
    {
        super( message );
    }

    public AuthorizationExpiredException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
