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
package org.neo4j.unsafe.impl.batchimport.executor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Strategy for waiting a while, given a certain {@link Thread}.
 */
public interface ParkStrategy
{
    void park( Thread thread );

    void unpark( Thread thread );

    class Park implements ParkStrategy
    {
        private final long nanos;

        public Park( long time, TimeUnit unit )
        {
            this.nanos = NANOSECONDS.convert( time, unit );
        }

        @Override
        public void park( Thread thread )
        {
            LockSupport.parkNanos( nanos );
        }

        @Override
        public void unpark( Thread thread )
        {
            LockSupport.unpark( thread );
        }
    }
}
