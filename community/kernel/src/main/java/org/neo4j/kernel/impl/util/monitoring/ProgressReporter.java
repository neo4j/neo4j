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
package org.neo4j.kernel.impl.util.monitoring;

/**
 * Progress indicator to track progress of long running processes.
 * Reporter should be configured with maximum number of progress steps, by starting it.
 * Each small step of long running task then responsible to call {@link #progress(long)} to inform about ongoing
 * progress.
 * In the end {@link #completed()} should be invoked to signal about execution completion.
 */
public interface ProgressReporter
{
    /**
     * @param max max progress, which {@link #progress(long)} moves towards.
     */
    void start( long max );

    /**
     * Percentage completeness for the current section.
     *
     * @param add progress to add towards a maximum.
     */
    void progress( long add );

    /**
     * Called if this section was completed successfully.
     */
    void completed();
}
