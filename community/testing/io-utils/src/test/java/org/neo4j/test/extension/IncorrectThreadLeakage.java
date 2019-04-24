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
package org.neo4j.test.extension;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class IncorrectThreadLeakage
{

    private static Thread thread;
    private static AtomicBoolean stop = new AtomicBoolean( false );

    @Test
    void leakThreads()
    {
        thread = new Thread( () ->
        {
            while ( !stop.get() )
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
        } );
        thread.setName( "Intentionally Leaked Thread" );
        thread.start();
    }

    public static void cleanUp() throws InterruptedException
    {
        stop.set( true );
        thread.join();
    }
}
