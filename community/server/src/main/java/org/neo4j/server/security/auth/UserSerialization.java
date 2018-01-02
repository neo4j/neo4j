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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.kernel.impl.util.Codecs;

import static java.lang.String.format;

/**
 * Serializes user authorization and authentication data to a format similar to unix passwd files.
 */
public class UserSerialization
{
    public class FormatException extends Exception
    {
        FormatException( String message )
        {
            super( message );
        }
    }

    private static final String userSeparator = ":";
    private static final String credentialSeparator = ",";

    public byte[] serialize(Collection<User> users)
    {
        StringBuilder sb = new StringBuilder();
        for ( User user : users )
        {
            sb.append( serialize(user) ).append( "\n" );
        }
        return sb.toString().getBytes( Charsets.UTF_8 );
    }

    public List<User> deserializeUsers( byte[] bytes ) throws FormatException
    {
        List<User> out = new ArrayList<>();
        int lineNumber = 1;
        for ( String line : new String( bytes, Charsets.UTF_8 ).split( "\n" ) )
        {
            if (line.trim().length() > 0)
            {
                out.add( deserializeUser( line, lineNumber ) );
            }
            lineNumber++;
        }
        return out;
    }

    private String serialize( User user )
    {
        return join( userSeparator, user.name(),
                serialize( user.credentials() ),
                user.passwordChangeRequired() ? "password_change_required" : "" );
    }

    private User deserializeUser( String line, int lineNumber ) throws FormatException
    {
        String[] parts = line.split( userSeparator, -1 );
        if ( parts.length != 3 )
        {
            throw new FormatException( format( "wrong number of line fields [line %d]", lineNumber ) );
        }
        return new User.Builder()
                .withName( parts[0] )
                .withCredentials( deserializeCredentials( parts[1], lineNumber ) )
                .withRequiredPasswordChange( parts[2].equals( "password_change_required" ) )
                .build();
    }

    private String serialize( Credential cred )
    {
        String encodedSalt = Codecs.encodeHexString( cred.salt() );
        String encodedPassword = Codecs.encodeHexString( cred.passwordHash() );
        return join( credentialSeparator, Credential.DIGEST_ALGO, encodedPassword, encodedSalt );
    }

    private Credential deserializeCredentials( String part, int lineNumber ) throws FormatException
    {
        String[] split = part.split( credentialSeparator, -1 );
        if ( split.length != 3 )
        {
            throw new FormatException( format( "wrong number of credential fields [line %d]", lineNumber ) );
        }
        if ( !split[0].equals( Credential.DIGEST_ALGO ) )
        {
            throw new FormatException( format( "unknown digest \"%s\" [line %d]", split[0], lineNumber ) );
        }
        byte[] decodedPassword = Codecs.decodeHexString( split[1] );
        byte[] decodedSalt = Codecs.decodeHexString( split[2] );
        return new Credential( decodedSalt, decodedPassword );
    }

    private String join( String separator, String... segments )
    {
        StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < segments.length; i++ )
        {
            if(i > 0) { sb.append( separator ); }
            sb.append( segments[i] == null ? "" : segments[i] );
        }
        return sb.toString();
    }
}
