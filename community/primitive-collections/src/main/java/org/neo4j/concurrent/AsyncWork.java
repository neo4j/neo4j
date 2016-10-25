/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.util.concurrent.ExecutionException;

/**
 * The eventual or past completion of a unit of work submitted to the {@link WorkSync#applyAsync(Work)} method.
 */
public interface AsyncWork
{
    /**
     * Probes the status of the submitted work, and attempt to immediately complete it if it hasn't been already.
     *
     * @return {@code true} if the work was already completed when calling this method, or has been completed as a
     * result of calling this method. {@code false} if the work is not yet complete, and it could not be completed
     * immediately. The work cannot be immediately completed if the lock guarding the single-threaded work application
     * inside the {@link WorkSync} was already taken. This implies that another thread is already busy applying work.
     * @throws ExecutionException If this thread ended up applying the work immediately, but doing so threw an
     * exception. The thrown exception is attached as a cause of the {@link ExecutionException}.
     */
    boolean tryComplete() throws ExecutionException;
}
