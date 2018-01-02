/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.security.auth;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Clock;

public class RateLimitedAuthenticationStrategy implements AuthenticationStrategy
{
    private static final int FAILED_AUTH_COOLDOWN_PERIOD = 5_000;
    private final Clock clock;
    private final int maxFailedAttempts;

    private class AuthenticationMetadata
    {
        private final AtomicInteger failedAuthAttempts = new AtomicInteger();
        private long lastFailedAttemptTime = 0;

        public boolean authenticationPermitted()
        {
            return failedAuthAttempts.get() < maxFailedAttempts
                    || clock.currentTimeMillis() >= ( lastFailedAttemptTime + FAILED_AUTH_COOLDOWN_PERIOD );
        }

        public void authSuccess()
        {
            failedAuthAttempts.set( 0 );
        }

        public void authFailed()
        {
            failedAuthAttempts.incrementAndGet();
            lastFailedAttemptTime = clock.currentTimeMillis();
        }
    }

    /**
     * Tracks authentication state for each user
     */
    private final ConcurrentMap<String, AuthenticationMetadata> authenticationData = new ConcurrentHashMap<>();

    public RateLimitedAuthenticationStrategy( Clock clock, int maxFailedAttempts )
    {
        this.clock = clock;
        this.maxFailedAttempts = maxFailedAttempts;
    }

    public AuthenticationResult authenticate( User user, String password )
    {
        AuthenticationMetadata authMetadata = authMetadataFor( user );

        if ( !authMetadata.authenticationPermitted() )
        {
            return AuthenticationResult.TOO_MANY_ATTEMPTS;
        }

        if ( user.credentials().matchesPassword( password ) )
        {
            authMetadata.authSuccess();
            return AuthenticationResult.SUCCESS;
        } else
        {
            authMetadata.authFailed();
            return AuthenticationResult.FAILURE;
        }
    }

    private AuthenticationMetadata authMetadataFor( User user )
    {
        String username = user.name();
        AuthenticationMetadata authMeta = authenticationData.get( username );

        if ( authMeta == null )
        {
            authMeta = new AuthenticationMetadata();
            AuthenticationMetadata preExisting = authenticationData.putIfAbsent( username, authMeta );
            if ( preExisting != null )
            {
                authMeta = preExisting;
            }
        }

        return authMeta;
    }

}
