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
package org.neo4j.util.concurrent;

import org.neo4j.internal.helpers.Exceptions;

public class Runnables
{
    public static final Runnable EMPTY_RUNNABLE = () ->
    {
        // empty
    };

    /**
     * Run all runnables, chaining exceptions, if any, into a single {@link RuntimeException} with provided message as message.
     * If there's only a single failure and it's a {@link RuntimeException} it will be thrown as-is.
     * @param message passed to resulting {@link RuntimeException} if any runnable throw.
     * @param runnables to run.
     */
    public static void runAll( String message, Runnable... runnables )
    {
        Throwable exceptions = null;
        int errors = 0;
        for ( Runnable runnable : runnables )
        {
            try
            {
                runnable.run();
            }
            catch ( Throwable t )
            {
                exceptions = Exceptions.chain( exceptions, t );
                errors++;
            }
        }
        if ( exceptions != null )
        {
            if ( errors == 1 )
            {
                Exceptions.throwIfUnchecked( exceptions );
            }
            throw new RuntimeException( message, exceptions );
        }
    }
}
