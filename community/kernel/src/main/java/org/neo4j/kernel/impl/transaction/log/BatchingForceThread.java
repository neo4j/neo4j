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
import java.util.concurrent.locks.LockSupport;

/**
 * Background thread that goes together with {@link BatchingPhysicalTransactionAppender} and performs
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
        boolean force() throws IOException;
    }
    
    private volatile boolean run = true;
    private final Operation operation;
    private volatile IOException failure;
    
    BatchingForceThread( Operation operation )
    {
        super( "BatchingWrites thread" );
        this.operation = operation;
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
                if ( !operation.force() )
                {
                    LockSupport.parkNanos( 1_000_000 ); // 1 ms
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
