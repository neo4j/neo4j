/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.com;

import org.neo4j.logging.Log;

public class ComException extends RuntimeException
{
    public static final boolean TRACE_HA_CONNECTIVITY = Boolean.getBoolean( "org.neo4j.com.TRACE_HA_CONNECTIVITY" );

    public ComException()
    {
        super();
    }

    public ComException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ComException( String message )
    {
        super( message );
    }

    public ComException( Throwable cause )
    {
        super( cause );
    }

    public ComException traceComException( Log log, String tracePoint )
    {
        if ( TRACE_HA_CONNECTIVITY )
        {
            String msg = String.format( "ComException@%x trace from %s: %s",
                    System.identityHashCode( this ), tracePoint, getMessage() );
            log.debug( msg, this, true );
        }
        return this;
    }
}
