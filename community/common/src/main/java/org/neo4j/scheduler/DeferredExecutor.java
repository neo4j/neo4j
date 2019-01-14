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
package org.neo4j.scheduler;

import java.util.concurrent.Executor;

/**
 * Defers execution of all tasks sent to it until it is "satisfied" by being
 * provided with an actual executor.
 * <p>
 * This can be used to construct services that need an executor for their
 * constructor who wish to defer construction of actual executors to some other
 * time or place.
 * <p>
 * You should also not use this when there is a risk that not executing tasks
 * could block the progress of the application lifecycle.
 * <p>
 */
public interface DeferredExecutor extends Executor
{
    void satisfyWith( Executor executor );
}
