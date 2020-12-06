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
package org.neo4j.shell.util;

import org.neo4j.driver.exceptions.Neo4jException;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

@SuppressWarnings( {"WeakerAccess", "unused"} )
public final class Versions
{

    private Versions()
    {
        throw new UnsupportedOperationException( "Don't instantiate" );
    }

    public static int majorVersion( String version )
    {
        return version( version ).major();
    }

    public static int minorVersion( String version )
    {
        return version( version ).minor();
    }

    public static int patch( String version )
    {
        return version( version ).patch();
    }

    public static Version version( String version )
    {
        if ( version == null )
        {
            throw new AssertionError( "null is not a valid version string" );
        }
        if ( version.isEmpty() )
        {
            return new Version( 0, 0, 0 );
        }
        //remove -alpha, and -beta etc
        int offset = version.indexOf( "-" );
        if ( offset > 0 )
        {
            version = version.substring( 0, offset );
        }
        String[] split = version.split( "\\." );
        switch ( split.length )
        {
        case 1:
            return new Version( parseInt( split[0] ), 0, 0 );
        case 2:
            return new Version( parseInt( split[0] ), parseInt( split[1] ), 0 );
        case 3:
            return new Version( parseInt( split[0] ), parseInt( split[1] ), parseInt( split[2] ) );
        default:
            throw new AssertionError(
                    format( "%s is not a proper version string, it should be of the form X.Y.Z ", version ) );
        }
    }

    public static boolean isPasswordChangeRequiredException( Neo4jException e )
    {
        return "Neo.ClientError.Security.CredentialsExpired".equalsIgnoreCase( e.code() );
    }
}
