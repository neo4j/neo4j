/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.server.security.auth.exception.IllegalTokenException;
import org.neo4j.server.security.auth.exception.IllegalUsernameException;
import org.neo4j.server.security.auth.exception.TooManyAuthenticationAttemptsException;

import static org.neo4j.kernel.impl.util.BytePrinter.compactHex;

/**
 * Controls authentication, you can use this class to verify if a client has valid user credentials for the user she
 * claims to represent. This class also allows changing those credentials.
 *
 * Note that it crucially does not deal with authorization, including authorization tokens. These are handled by
 * {@link org.neo4j.server.security.auth.SecurityCentral}.
 */
public class Authentication
{
    private final String DIGEST_ALGO = "SHA-256";

    private class AuthenticationMetadata
    {
        private final AtomicInteger failedAuthAttempts = new AtomicInteger();
        private final String name;
        private final int maxFailedAttempts;
        private final long failedCooldownPeriod;
        private final Clock clock;

        private long lastFailedAttemptTime = 0;

        AuthenticationMetadata( String name, int maxFailedAttempts, long failedCooldownPeriod, Clock clock )
        {
            this.name = name;
            this.maxFailedAttempts = maxFailedAttempts;
            this.failedCooldownPeriod = failedCooldownPeriod;
            this.clock = clock;
        }

        public boolean authenticate( String password ) throws TooManyAuthenticationAttemptsException
        {
            if( tooManyAuthAttemtps() )
            {
                throw new TooManyAuthenticationAttemptsException( "Too many failed authentication requests. Please try again in 5 seconds." );
            }
            if( isCorrectPassword( password ) )
            {
                failedAuthAttempts.set( 0 );
                return true;
            }
            else
            {
                failedAuthAttempts.incrementAndGet();
                lastFailedAttemptTime = clock.currentTimeMillis();
                return false;
            }
        }

        private boolean tooManyAuthAttemtps()
        {
            return failedAuthAttempts.get() >= maxFailedAttempts
                    && clock.currentTimeMillis() < (lastFailedAttemptTime + failedCooldownPeriod);
        }

        protected boolean isCorrectPassword( String password )
        {
            User user = users.findByName( name );
            if(user != null)
            {
                String hash = hash( user.credentials().salt(), password, user.credentials().digestAlgorithm() );
                return hash.equals( user.credentials().hash() );
            }
            return false;
        }
    }

    private class UnknownUserMetadata extends AuthenticationMetadata
    {

        UnknownUserMetadata( int maxFailedAttempts, long failedCooldownPeriod, Clock clock )
        {
            super( "Unknown", maxFailedAttempts, failedCooldownPeriod, clock );
        }

        @Override
        protected boolean isCorrectPassword( String password )
        {
            return false;
        }
    }

    private final AuthenticationMetadata unknownUser;
    private final int failedAuthCooldownPeriod = 5_000;
    private final Clock clock;
    private final int maxFailedAttempts;
    private final SecureRandom rand = new SecureRandom();

    /** Storage for data about principals */
    private final UserRepository users;

    /** Tracks authentication objects for each user, including tracking of authentication attempts. */
    private final ConcurrentMap<String, AuthenticationMetadata> authenticationData = new ConcurrentHashMap<>();

    public Authentication( Clock clock, UserRepository users, int maxFailedAttempts )
    {
        this.clock = clock;
        this.users = users;
        this.maxFailedAttempts = maxFailedAttempts;
        this.unknownUser = new UnknownUserMetadata( maxFailedAttempts, failedAuthCooldownPeriod, clock );
    }

    /** Verify that a user name and password combo is valid. */
    public boolean authenticate( String name, String password ) throws TooManyAuthenticationAttemptsException
    {
        return authMetadataFor( name ).authenticate( password );
    }

    public void setPassword( String name, String password ) throws IOException
    {
        User user = users.findByName( name );
        if(user != null)
        {
            try
            {
                String salt = randomSalt();
                users.save( user.augment()
                        .withCredentials( new Credentials( salt, DIGEST_ALGO, hash( salt, password, DIGEST_ALGO ) ) )
                        .withRequiredPasswordChange( false )
                        .build());
            }
            catch ( IllegalTokenException | IllegalUsernameException e )
            {
                throw new ThisShouldNotHappenError( "Jake", "Token/username are not being modified.", e );
            }
        }
        else
        {
            throw new RuntimeException( "No such user: " + name );
        }
    }

    /** Mark the user with the specified name as requiring a password change. All API access will be blocked until the password is changed. */
    public void requirePasswordChange( String name ) throws IOException
    {
        User user = users.findByName( name );
        if(user != null)
        {
            try
            {
                users.save(user.augment().withRequiredPasswordChange( true ).build());
            }
            catch ( IllegalTokenException | IllegalUsernameException e )
            {
                throw new ThisShouldNotHappenError( "Jake", "Token/username are not being modified.", e );
            }
        }
        else
        {
            throw new RuntimeException( "No such user: " + name );
        }
    }

    private AuthenticationMetadata authMetadataFor( String name )
    {
        if(name == null)
        {
            return unknownUser;
        }

        AuthenticationMetadata authMeta = authenticationData.get( name );
        if(authMeta == null)
        {
            User user = users.findByName( name );
            if ( user != null )
            {
                authMeta = new AuthenticationMetadata( name, maxFailedAttempts, failedAuthCooldownPeriod, clock );
                AuthenticationMetadata preExisting = authenticationData.putIfAbsent( name, authMeta );
                if(preExisting != null)
                {
                    authMeta = preExisting;
                }
            }
            else
            {
                authMeta = unknownUser;
            }
        }
        return authMeta;
    }

    private String randomSalt()
    {
        byte[] salt = new byte[16];
        rand.nextBytes( salt );
        return compactHex( salt );
    }

    private String hash( String salt, String password, String digestAlgo)
    {
        try
        {
            byte[] bytes = (salt+password).getBytes( Charsets.UTF_8 );
            MessageDigest m = MessageDigest.getInstance( digestAlgo );
            m.update( bytes, 0, bytes.length);
            return compactHex( m.digest() );
        }
        catch ( NoSuchAlgorithmException e )
        {
           throw new RuntimeException( "Hash algorithm is not available on this platform: " + e.getMessage(),e );
        }
    }
}
