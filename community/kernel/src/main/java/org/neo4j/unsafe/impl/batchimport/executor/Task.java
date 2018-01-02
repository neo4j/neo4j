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
package org.neo4j.unsafe.impl.batchimport.executor;

import java.util.concurrent.Callable;

/**
 * Task submitted to a {@link TaskExecutor}, similar to {@link Callable} or {@link Runnable},
 * but tailored to {@link TaskExecutor} in that f.ex. it {@link #run(Object) runs} with a pre-defined
 * thread-local state as parameter.
 *
 * @param <LOCAL> thread-local state provided by the {@link TaskExecutor} executing this task.
 */
public interface Task<LOCAL>
{
    void run( LOCAL threadLocalState ) throws Exception;
}
