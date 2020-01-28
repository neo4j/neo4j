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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.neo4j.util.VisibleForTesting;

public class CallableExecutorService implements CallableExecutor
{
    private final ExecutorService executorService;

    public CallableExecutorService( ExecutorService executorService )
    {
        this.executorService = executorService;
    }

    @Override
    public <T> Future<T> submit( Callable<T> callable )
    {
        return executorService.submit( callable );
    }

    @Override
    public void execute( Runnable command )
    {
        executorService.submit( command );
    }

    @VisibleForTesting
    public Object delegate()
    {
        return executorService;
    }
}
