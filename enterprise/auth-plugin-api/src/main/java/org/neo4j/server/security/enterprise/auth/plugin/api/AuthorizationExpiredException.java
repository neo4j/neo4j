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
