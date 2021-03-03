/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.lock;

import java.util.concurrent.locks.LockSupport;

import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Interrupted;

public enum LockWaitStrategies implements WaitStrategy
{
    SPIN
    {
        @Override
        public void apply( long iteration ) throws AcquireLockTimeoutException
        {
            Thread.onSpinWait();
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
        private static final int spinIterations = 1000;
        private static final long multiplyUntilIteration = spinIterations + 2;

        @Override
        public void apply( long iteration ) throws AcquireLockTimeoutException
        {
            if ( iteration < spinIterations )
            {
                SPIN.apply( iteration );
                return;
            }

            try
            {
                if ( iteration < multiplyUntilIteration )
                {
                    Thread.sleep( 1 );
                }
                else
                {
                    LockSupport.parkNanos( 500 );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new AcquireLockTimeoutException( "Interrupted while waiting.", e, Interrupted );
            }
        }
    },
    NO_WAIT
    {
        @Override
        public void apply( long iteration )
                throws AcquireLockTimeoutException
        {
            // The NO_WAIT bail-out is a mix of deadlock and lock acquire timeout.
            throw new AcquireLockTimeoutException( "Cannot acquire lock, and refusing to wait.",
                    Status.Transaction.DeadlockDetected );
        }
    }
}
