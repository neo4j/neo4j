/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.scheduler;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

public interface JobHandle
{
    void cancel( boolean mayInterruptIfRunning );

    void waitTermination() throws InterruptedException, ExecutionException, CancellationException;

    default void registerCancelListener( CancelListener listener )
    {
        throw new UnsupportedOperationException( "Unsupported in this implementation" );
    }

    JobHandle nullInstance = new NullJobHandle();

    class NullJobHandle implements JobHandle
    {

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {   // no-op
        }

        @Override
        public void waitTermination() throws CancellationException
        {   // no-op
        }
    }
}
