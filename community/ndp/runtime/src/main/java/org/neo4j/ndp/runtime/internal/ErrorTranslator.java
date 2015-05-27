/*
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
package org.neo4j.ndp.runtime.internal;

import org.neo4j.cypher.CypherException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;

/**
 * Convert the mixed exceptions the underlying engine can throw to a cohesive set of known failures. This is an
 * intermediary mechanism.
 */
public class ErrorTranslator
{

    private final Log log;

    public ErrorTranslator( Log log )
    {
        this.log = log;
    }

    public Neo4jError translate( Throwable any )
    {
        if ( any instanceof KernelException )
        {
            return new Neo4jError( ((KernelException) any).status(), any.getMessage() );
        }
        else if ( any instanceof CypherException )
        {
            return new Neo4jError( ((CypherException) any).status(), any.getMessage() );
        }
        else
        {
            if ( any.getCause() != null )
            {
                return translate( any.getCause() );
            }

            // Log unknown errors.
            log.warn( "Client triggered unknown error: " + any.getMessage(), any );

            return new Neo4jError( Status.General.UnknownFailure, "An unexpected failure occurred: '"
                                                                  + any.getMessage() + "'." );
        }
    }

}
