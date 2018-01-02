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
package org.neo4j.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.Clock;

public class InputStreamAwaiter
{
    private final InputStream input;
    private final byte[] bytes = new byte[1024];
    private final Clock clock;

    public InputStreamAwaiter( InputStream input )
    {
        this( Clock.SYSTEM_CLOCK, input );
    }

    public InputStreamAwaiter( Clock clock, InputStream input )
    {
        this.clock = clock;
        this.input = input;
    }

    public void awaitLine( String expectedLine, long timeout, TimeUnit unit ) throws IOException,
            TimeoutException, InterruptedException
    {
        long deadline = clock.currentTimeMillis() + unit.toMillis( timeout );
        StringBuilder buffer = new StringBuilder();
        do
        {
            while ( input.available() > 0 )
            {
                buffer.append( new String( bytes, 0, input.read( bytes ) ) );
            }

            String[] lines = buffer.toString().split( System.lineSeparator() );
            for ( String line : lines )
            {
                if ( expectedLine.equals( line ) )
                {
                    return;
                }
            }

            Thread.sleep( 10 );
        }
        while ( clock.currentTimeMillis() < deadline );

        throw new TimeoutException( "Timed out waiting to read line: [" + expectedLine + "]. Seen input:\n\t"
                + buffer.toString().replaceAll( "\n", "\n\t" ) );
    }
}
