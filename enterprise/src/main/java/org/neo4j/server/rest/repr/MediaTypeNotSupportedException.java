/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest.repr;

import java.util.Collection;

import javax.ws.rs.core.MediaType;

public class MediaTypeNotSupportedException extends Exception
{
    public MediaTypeNotSupportedException( Collection<MediaType> supported, MediaType... requested )
    {
        super( buildMessage( supported, requested ) );
    }

    private static String buildMessage( Collection<MediaType> supported, MediaType[] requested )
    {
        StringBuilder message = new StringBuilder( "No matching representation format found.\n" );
        if ( requested.length == 0 )
        {
            message.append( "No requested representation format supplied." );
        }
        else if ( requested.length == 1 )
        {
            message.append( "Request format: " ).append( requested[0] ).append( "\n" );
        }
        else
        {
            message.append( "Requested formats:\n" );
            for ( int i = 0; i < requested.length; i++ )
            {
                message.append( " " ).append( i ).append( ". " );
                message.append( requested[i] ).append( "\n" );
            }
        }
        message.append( "Supported representation formats:" );
        if ( supported.isEmpty() )
        {
            message.append( " none" );
        }
        else
        {
            for ( MediaType type : supported )
            {
                message.append( "\n * " ).append( type );
            }
        }
        return message.toString();
    }
}
