/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v3.messaging.decoder;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.kernel.api.exceptions.Status;

public enum StatementMode //TODO is this already somewhere?
{
    READ( "R" ),
    WRITE( "W" );

    private final String signature;

    StatementMode( String name )
    {
        this.signature = name;
    }

    public String signature()
    {
        return this.signature;
    }

    public static StatementMode parseMode( String str ) throws BoltIOException
    {
        if ( str.equalsIgnoreCase( READ.signature() ) )
        {
            return READ;
        }
        else if ( str.equalsIgnoreCase( WRITE.signature() ) )
        {
            return WRITE;
        }
        throw new BoltIOException( Status.Request.InvalidFormat, String.format( "Unknown mode '%s' for input cypher statement.", str ) );
    }
}
