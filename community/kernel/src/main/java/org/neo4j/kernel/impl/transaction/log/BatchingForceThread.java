/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import static org.neo4j.kernel.impl.util.DebugUtil.trackTest;

/**
 * Background thread that goes together with {@link ParallelBatchingPhysicalTransactionAppender} and performs
 * and {@link Operation} as fast as it can as until it's {@link #halt() halted}.
 */
class BatchingForceThread extends Thread
{
    public interface Operation
    {
        /**
         * @return {@code true} if there were transactions that were forced to disk, otherwise {@code false}.
         * @throws IOException
         */
        boolean perform() throws IOException;
    }

    private volatile boolean run = true;
    private final Operation operation;
    private final WaitStrategy waitStrategy;
    private volatile IOException failure;

    /**
     * @param waitStrategy how do we wait if there's nothing in particular to do right now?
     */
    BatchingForceThread( Operation operation, WaitStrategy waitStrategy )
    {
        super( "BatchingWrites thread" + trackTest() );
        this.operation = operation;
        this.waitStrategy = waitStrategy;
        setDaemon( true );
    }

    public void halt()
    {
        run = false;
    }

    @Override
    public void run()
    {
        while ( run )
        {
            try
            {
                if ( !operation.perform() )
                {
                    waitStrategy.wait( this );
                }
            }
            catch ( IOException e )
            {
                failure = e;
                break;
            }
        }
    }

    boolean checkHealth() throws IOException
    {
        if ( failure != null )
        {
            throw new IOException( "An earlier force has failed", failure );
        }
        return true;
    }
}
