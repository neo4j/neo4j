/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

/**
 * Serializes user authorization and authentication data to a format similar to unix passwd files.
 */
public class UserSerialization
{
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

    public List<User> deserializeUsers( byte[] bytes )
    {
        List<User> out = new ArrayList<>();
        for ( String line : new String( bytes, Charsets.UTF_8 ).split( "\n" ) )
        {
            if(line.trim().length() > 0)
            {
                out.add( deserializeUser( line ) );
            }
        }
        return out;
    }

    private String serialize( User user )
    {
        return join( userSeparator, new String[]{
                user.name(),
                user.token(),
                serialize( user.credentials() ),
                user.passwordChangeRequired() ? "true" : "false"} );
    }

    private User deserializeUser( String line )
    {
        String[] parts = line.split( userSeparator, -1 );
        if(parts.length != 4)
        {
            throw new IllegalStateException( "Cannot read user data from authorization file." );
        }
        return new User.Builder()
                .withName( parts[0] )
                .withToken( parts[1] )
                .withCredentials( deserializeCredentials(parts[2]) )
                .withRequiredPasswordChange( parts[3].equals( "true" ) )
                .withPrivileges( Privileges.ADMIN ) // Only "real" privilege available right now
                .build();
    }

    private String serialize( Credentials cred )
    {
        return join( credentialSeparator, new String[]{cred.digestAlgorithm(), cred.hash(), cred.salt()} );
    }

    private Credentials deserializeCredentials( String part )
    {
        String[] split = part.split( credentialSeparator, -1 );
        if(split.length != 3)
        {
            throw new IllegalStateException( "Cannot read credential data from authorization file: " + part + " " + split.length + " '" + join(":", split) + "'");
        }
        return new Credentials( split[2], split[0], split[1] );
    }

    private String join( String separator, String[] segments )
    {
        StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < segments.length; i++ )
        {
            if(i > 0) { sb.append( separator ); }
            sb.append( segments[i] );
        }
        return sb.toString();
    }
}
