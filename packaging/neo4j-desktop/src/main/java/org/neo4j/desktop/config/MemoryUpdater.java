/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.config;

import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MemoryUpdater implements Runnable
{
    public static interface Reporter
    {
        /**
         * @param percentFull between 0..100
         */
        public void reportMemoryUsage( int percentFull );
    }
    
    private boolean halted = false;
    private final Reporter reporter;
    
    public MemoryUpdater( Reporter reporter )
    {
        this.reporter = reporter;
    }
    
    public void halt()
    {
        halted = true;
    }
    
    @Override
    public void run()
    {
        while ( !halted )
        {
            sleepAWhile();
            if ( !halted )
            {
                reporter.reportMemoryUsage( percentOf(
                        (float) Runtime.getRuntime().totalMemory() /
                        (float) Runtime.getRuntime().maxMemory() ) );
            }
        }
    }

    private int percentOf( float ratio )
    {
        return (int) round( ratio * 100.0 );
    }

    private void sleepAWhile()
    {
        long endTime = System.currentTimeMillis() + SECONDS.toMillis( 1 );
        while ( !halted && System.currentTimeMillis() < endTime )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                // hmm
            }
        }
    }
}
