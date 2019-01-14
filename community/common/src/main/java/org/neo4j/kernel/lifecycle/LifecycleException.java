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
package org.neo4j.kernel.lifecycle;

/**
 * This exception is thrown by LifeSupport if a lifecycle transition fails. If many exceptions occur
 * they will be chained through the cause exception mechanism.
 */
public class LifecycleException
    extends RuntimeException
{

    public LifecycleException( Object instance, LifecycleStatus from, LifecycleStatus to, Throwable cause )
    {
        super( humanReadableMessage( instance, from, to, cause ), cause );
    }

    public LifecycleException( String message, Throwable cause )
    {
        super( message, cause );
    }

    private static String humanReadableMessage( Object instance, LifecycleStatus from,
            LifecycleStatus to, Throwable cause )
    {
        String instanceStr = String.valueOf( instance );
        StringBuilder message = new StringBuilder();
        switch ( to )
        {
            case STOPPED:
                if ( from == LifecycleStatus.NONE )
                {
                    message.append( "Component '" ).append( instanceStr ).append( "' failed to initialize" );
                }
                else if ( from == LifecycleStatus.STARTED )
                {
                    message.append( "Component '" ).append( instanceStr ).append( "' failed to stop" );
                }
                break;
            case STARTED:
                if ( from == LifecycleStatus.STOPPED )
                {
                    message.append( "Component '" ).append( instanceStr )
                           .append( "' was successfully initialized, but failed to start" );
                }
                break;
            case SHUTDOWN:
                message.append( "Component '" ).append( instanceStr ).append( "' failed to shut down" );
                break;
            default:
                break;
        }
        if ( message.length() == 0 )
        {
            message.append( "Component '" ).append( instanceStr ).append( "' failed to transition from " )
                   .append( from.name().toLowerCase() ).append( " to " ).append( to.name().toLowerCase() );
        }
        message.append( '.' );
        if ( cause != null )
        {
            Throwable root = rootCause( cause );
            message.append( " Please see the attached cause exception \"" ).append( root.getMessage() ).append( '"' );
            if ( root.getCause() != null )
            {
                message.append( " (root cause cycle detected)" );
            }
            message.append( '.' );
        }

        return message.toString();
    }

    private static Throwable rootCause( Throwable cause )
    {
        int i = 0; // Guard against infinite self-cause exception-loops.
        while ( cause.getCause() != null && i++ < 100 )
        {
            cause = cause.getCause();
        }
        return cause;
    }
}
