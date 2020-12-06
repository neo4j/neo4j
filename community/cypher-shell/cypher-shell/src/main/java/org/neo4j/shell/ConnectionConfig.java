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
package org.neo4j.shell;

import javax.annotation.Nonnull;

import org.neo4j.driver.v1.Config;

public class ConnectionConfig
{
    private final String scheme;
    private final String host;
    private final int port;
    private final Config.EncryptionLevel encryption;
    private String username;
    private String password;

    public ConnectionConfig( @Nonnull String scheme, @Nonnull String host, int port,
                             @Nonnull String username, @Nonnull String password, boolean encryption )
    {
        this.host = host;
        this.port = port;
        this.username = fallbackToEnvVariable( username, "NEO4J_USERNAME" );
        this.password = fallbackToEnvVariable( password, "NEO4J_PASSWORD" );
        this.encryption = encryption ? Config.EncryptionLevel.REQUIRED : Config.EncryptionLevel.NONE;
        this.scheme = scheme;
    }

    /**
     * @return preferredValue if not empty, else the contents of the fallback environment variable
     */
    @Nonnull
    static String fallbackToEnvVariable( @Nonnull String preferredValue, @Nonnull String fallbackEnvVar )
    {
        String result = System.getenv( fallbackEnvVar );
        if ( result == null || !preferredValue.isEmpty() )
        {
            result = preferredValue;
        }
        return result;
    }

    @Nonnull
    public String scheme()
    {
        return scheme;
    }

    @Nonnull
    public String host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    @Nonnull
    public String username()
    {
        return username;
    }

    @Nonnull
    public String password()
    {
        return password;
    }

    @Nonnull
    public String driverUrl()
    {
        return String.format( "%s%s:%d", scheme(), host(), port() );
    }

    @Nonnull
    public Config.EncryptionLevel encryption()
    {
        return encryption;
    }

    public void setUsername( @Nonnull String username )
    {
        this.username = username;
    }

    public void setPassword( @Nonnull String password )
    {
        this.password = password;
    }
}
