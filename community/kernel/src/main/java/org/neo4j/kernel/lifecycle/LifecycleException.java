/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.lifecycle;

/**
 * This exception is thrown by LifeSupport if a lifecycle transition fails. If many exceptions occur
 * they will be chained through the cause exception mechanism.
 */
public class LifecycleException
    extends RuntimeException
{

    private static final String humanReadableMessage( Object instance, LifecycleStatus from, LifecycleStatus to )
    {
        switch(to)
        {
            case STOPPED:
                if(from == LifecycleStatus.NONE)
                {
                    return String.format( "Component '%s' failed to initialize. Please see attached cause exception" +
                            ".", instance.toString() );
                }
                if(from == LifecycleStatus.STARTED)
                {
                    return String.format( "Component '%s' failed to stop. Please see attached cause exception.",
                            instance.toString()  );
                }
                break;
            case STARTED:
                if(from == LifecycleStatus.STOPPED)
                {
                    return String.format( "Component '%s' was successfully initialized, but failed to start. Please " +
                            "see attached cause exception.", instance.toString() );
                }
                break;
            case SHUTDOWN:
                return String.format( "Component '%s' failed to shut down. Please " +
                        "see attached cause exception.", instance.toString() );
        }

        return String.format( "Failed to transition component '%s' from %s to %s. Please see attached cause exception",
                instance.toString(), from.name(), to.name() );
    }

    public LifecycleException( Object instance, LifecycleStatus from, LifecycleStatus to, Throwable cause )
    {
        super( humanReadableMessage( instance, from, to ), cause);
    }
}
