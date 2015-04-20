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
package org.neo4j.test;

import static java.lang.String.format;

import static org.junit.Assert.fail;

public class AssertEventually
{
    public static void assertEventually( String message, int timeoutSeconds, Condition condition )
    {
        long startTime = System.currentTimeMillis();
        while ( ! condition.evaluate() )
        {
            long currentTime = System.currentTimeMillis();
            if ( currentTime - startTime > timeoutSeconds * 1000 )
            {
                fail( format( "Condition '%s' was not true after %s seconds.", message, timeoutSeconds ) );
            }
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException ignored )
            {
            }
        }
    }

    public interface Condition
    {
        boolean evaluate();
    }
}
