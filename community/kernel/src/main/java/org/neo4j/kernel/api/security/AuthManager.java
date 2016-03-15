/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.security;

import java.io.IOException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;

/**
 * An AuthManager is used to do basic authentication and user management.
 */
public interface AuthManager extends Lifecycle
{
    abstract class Factory extends Service
    {
        public Factory( String key, String... altKeys )
        {
            super( key, altKeys );
        }

        public abstract AuthManager newInstance( Config config, LogProvider logProvider );
    }

    /**
     * Authenticate a username and password
     * @param username The name of the user
     * @param password The password of the user
     */
    AuthenticationResult authenticate( String username, String password );

    /**
     * Log in using the provided username and password
     * @param username The name of the user
     * @param password The password of the user
     * @return An AuthSubject representing the newly logged-in user
     */
    AuthSubject login( String username, String password );

    /**
     * Implementation that does no authentication.
     */
    AuthManager NO_AUTH = new AuthManager()
    {
        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void stop() throws Throwable
        {
        }

        @Override
        public void shutdown() throws Throwable
        {
        }

        @Override
        public AuthenticationResult authenticate( String username, String password )
        {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public AuthSubject login( String username, String password )
        {
            return new AuthSubject()
            {
                @Override
                public void logout()
                {
                }

                @Override
                public AuthenticationResult getAuthenticationResult()
                {
                    return AuthenticationResult.SUCCESS;
                }

                @Override
                public void setPassword( String password ) throws IOException
                {
                }

                @Override
                public boolean allowsReads()
                {
                    return true;
                }

                @Override
                public boolean allowsWrites()
                {
                    return true;
                }

                @Override
                public boolean allowsSchemaWrites()
                {
                    return true;
                }

                @Override
                public String name()
                {
                    return "";
                }
            };
        }
    };
}
