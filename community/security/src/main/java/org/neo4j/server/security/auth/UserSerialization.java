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
package org.neo4j.server.security.auth;

import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.exception.FormatException;
import org.neo4j.string.HexString;

import static java.lang.String.format;

/**
 * Serializes user authorization and authentication data to a format similar to unix passwd files.
 */
public class UserSerialization extends FileRepositorySerializer<User>
{
    private static final String userSeparator = ":";
    private static final String credentialSeparator = ",";

    @Override
    protected String serialize( User user )
    {
        return String.join( userSeparator,
                user.name(),
                serialize( user.credentials() ),
                String.join( ",", user.getFlags() )
            );
    }

    @Override
    protected User deserializeRecord( String line, int lineNumber ) throws FormatException
    {
        String[] parts = line.split( userSeparator, -1 );
        if ( parts.length != 3 )
        {
            throw new FormatException( format( "wrong number of line fields, expected 3, got %d [line %d]",
                    parts.length,
                    lineNumber
            ) );
        }

        User.Builder b = new User.Builder()
                .withName( parts[0] )
                .withCredentials( deserializeCredentials( parts[1], lineNumber ) );

        for ( String flag : parts[2].split( ",", -1 ) )
        {
            String trimmed = flag.trim();
            if ( !trimmed.isEmpty() )
            {
                b = b.withFlag( trimmed );
            }
        }

        return  b.build();
    }

    private String serialize( Credential cred )
    {
        String encodedSalt = HexString.encodeHexString( cred.salt() );
        String encodedPassword = HexString.encodeHexString( cred.passwordHash() );
        return String.join( credentialSeparator, Credential.DIGEST_ALGO, encodedPassword, encodedSalt );
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
        byte[] decodedPassword = HexString.decodeHexString( split[1] );
        byte[] decodedSalt = HexString.decodeHexString( split[2] );
        return new Credential( decodedSalt, decodedPassword );
    }
}
