/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.neo4j.com.ComException;
import org.neo4j.kernel.impl.util.StringLogger;

public class UnknownReevaluationCause
{
    private static final String UNKNOWN = "UNKNOWN";
    
    public static ReevaluationCause fromDescription( final String description )
    {
        return new ReevaluationCause()
        {
            @Override
            public void log( StringLogger logger, String message )
            {
                logger.logMessage( formLogLine( message, this ), true );
            }

            @Override
            public String name()
            {
                return UNKNOWN;
            }

            @Override
            public String getDescription()
            {
                return description;
            }
        };
    }
    
    static String formLogLine( String message, ReevaluationCause cause )
    {
        return message + " Cause: " + cause.name() + (cause.getDescription() != null ? ": " + cause.getDescription() : "");
    }

    public static ReevaluationCause fromException( final Throwable cause )
    {
        // Here follows a bit of ugly instanceof checks because of the way
        // HA does reevaluation in general, which is: if we get an exception => reevaluate
        if ( cause instanceof ComException && cause.getCause() instanceof BranchedDataException )
            return KnownReevaluationCauses.BRANCHED_DATA;
        // ... add more here
        
        return new ReevaluationCause()
        {
            @Override
            public String name()
            {
                return UNKNOWN;
            }

            @Override
            public String getDescription()
            {
                return "See exception";
            }

            @Override
            public void log( StringLogger logger, String message )
            {
                logger.logMessage( formLogLine( message, this ), cause, true );
            }
        };
    }
}
