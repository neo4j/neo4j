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
package org.neo4j.consistency.checking.full;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.progress.Completion;

public enum TaskExecutionOrder
{
    MULTI_THREADED
    {
        @Override
        void execute( List<StoppableRunnable> tasks, Completion completion )
                throws ConsistencyCheckIncompleteException
        {
            ExecutorService executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
            for ( StoppableRunnable task : tasks )
            {
                executor.submit( task );
            }

            try
            {
                completion.await( 7, TimeUnit.DAYS );
            }
            catch ( Exception e )
            {
                tasks.get( 0 ).stopScanning();
                throw new ConsistencyCheckIncompleteException( e );
            }
            finally
            {
                executor.shutdown();
                try
                {
                    executor.awaitTermination( 10, TimeUnit.SECONDS );
                }
                catch ( InterruptedException e )
                {
                    // don't care
                }
            }
        }
    },
    SINGLE_THREADED
    {
        @Override
        void execute( List<StoppableRunnable> tasks, Completion completion )
                throws ConsistencyCheckIncompleteException
        {
            try
            {
                for ( StoppableRunnable task : tasks )
                {
                    task.run();
                }
                completion.await( 0, TimeUnit.SECONDS );
            }
            catch ( Exception e )
            {
                throw new ConsistencyCheckIncompleteException( e );
            }
        }
    },
    MULTI_PASS
    {
        @Override
        void execute( List<StoppableRunnable> tasks, Completion completion )
                throws ConsistencyCheckIncompleteException
        {
            try
            {
                for ( StoppableRunnable task : tasks )
                {
                    task.run();
                }
                completion.await( 0, TimeUnit.SECONDS );
            }
            catch ( Exception e )
            {
                throw new ConsistencyCheckIncompleteException( e );
            }
        }
    };

    abstract void execute( List<StoppableRunnable> tasks, Completion completion )
            throws ConsistencyCheckIncompleteException;
}
