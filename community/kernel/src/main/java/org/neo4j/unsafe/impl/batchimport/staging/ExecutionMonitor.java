/**
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
package org.neo4j.unsafe.impl.batchimport.staging;

/**
 * Monitors a {@link StageExecution}. An {@link ExecutionMonitor}, providing or displaying statistics
 * about the execution as it progresses.
 */
public interface ExecutionMonitor
{
    /**
     * Called when a {@link Stage} has started its execution. This method should not return until all
     * {@link Step steps} in the {@link Stage} have {@link Step#isCompleted() completed} their processing.
     *
     * @param executions execution of a {@link Stage}.
     */
    void monitor( StageExecution... executions );

    void done( long totalTimeMillis );
}
