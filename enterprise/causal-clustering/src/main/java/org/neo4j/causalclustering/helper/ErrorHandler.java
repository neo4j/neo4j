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
package org.neo4j.causalclustering.helper;

import java.util.ArrayList;
import java.util.List;

public class ErrorHandler implements AutoCloseable
{
    private final List<Throwable> throwables = new ArrayList<>();
    private final String message;

    /**
     * Ensures each action is executed. Any throwables will be saved and thrown after all actions have been executed. The first caught throwable will be cause
     * and any other will be added as suppressed.
     *
     * @param description The exception message if any are thrown.
     * @param actions Throwing runnables to execute.
     * @throws RuntimeException thrown if any action throws after all have been executed.
     */
    public static void runAll( String description, ThrowingRunnable... actions ) throws RuntimeException
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( description ) )
        {
            for ( ThrowingRunnable action : actions )
            {
                try
                {
                    action.run();
                }
                catch ( Throwable e )
                {
                    errorHandler.add( e );
                }
            }
        }
    }

    public ErrorHandler( String message )
    {
        this.message = message;
    }

    public void add( Throwable throwable )
    {
        throwables.add( throwable );
    }

    @Override
    public void close() throws RuntimeException
    {
        throwIfException();
    }

    private void throwIfException()
    {
        if ( !throwables.isEmpty() )
        {
            RuntimeException runtimeException = null;
            for ( Throwable throwable : throwables )
            {
                if ( runtimeException == null )
                {
                    runtimeException = new RuntimeException( message, throwable );
                }
                else
                {
                    runtimeException.addSuppressed( throwable );
                }
            }
            throw runtimeException;
        }
    }

    public interface ThrowingRunnable
    {
        void run() throws Throwable;
    }
}
