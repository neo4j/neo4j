/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup.util;

import static java.lang.String.format;

public class TestHelpers
{
    public static Exception executionIsExpectedToFail( Runnable runnable )
    {
        return executionIsExpectedToFail( runnable, RuntimeException.class );
    }

    public static <E extends Exception> E executionIsExpectedToFail( Runnable runnable, Class<E> exceptionClass )
    {
        try
        {
            runnable.run();
        }
        catch ( Exception e )
        {
            if ( !exceptionClass.isInstance( e ) )
            {
                throw new AssertionError( format( "Exception %s is not of type %s", e.getClass().getName(), exceptionClass.getName() ), e );
            }
            return (E) e;
        }
        throw new AssertionError( "The code expected to fail hasn't failed" );
    }
}
