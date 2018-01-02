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
package org.neo4j.kernel.impl.util.concurrent;

import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;

public enum LockWaitStrategies implements WaitStrategy<AcquireLockTimeoutException>
{
    SPIN
    {
        @Override
        public void apply( long iteration ) throws AcquireLockTimeoutException
        {
            // TODO We can experiment with introducing branch mispredictions here, to create
            // TODO bubbles in the pipeline that'll allow hyper-threaded threads on the
            // TODO same core to make progress. We can do that by generating a random number,
            // TODO e.g. with XorShift, and based on that, randomly branch to do a volatile
            // TODO write to one of two fields. The volatile fields would be there to give
            // TODO side-effect to the operation, preventing dead-code elimination.
        }
    },
    YIELD
    {
        @Override
        public void apply( long iteration ) throws AcquireLockTimeoutException
        {
            Thread.yield();
        }
    },
    INCREMENTAL_BACKOFF
    {
        private final int spinIterations = 1000;
        private final long multiplyUntilIteration = spinIterations + 2;

        @Override
        public void apply( long iteration ) throws AcquireLockTimeoutException
        {
            if(iteration < spinIterations)
            {
                SPIN.apply( iteration );
                return;
            }

            try
            {
                if(iteration < multiplyUntilIteration )
                {
                    Thread.sleep( 0, 1 << (iteration - spinIterations) );
                }
                else
                {
                    Thread.sleep( 0, 500 );
                }
            }
            catch(InterruptedException e)
            {
                Thread.interrupted();
                throw new AcquireLockTimeoutException( e , "Interrupted while waiting.");
            }
        }
    };
}
