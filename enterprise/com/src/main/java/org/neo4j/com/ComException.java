/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
            log.debug( msg, this );
        }
        return this;
    }
}
