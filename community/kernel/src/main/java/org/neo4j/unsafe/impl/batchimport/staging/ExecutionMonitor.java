/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * Gets notified now and then about {@link StageExecution}, where statistics can be read and displayed,
 * aggregated or in other ways make sense of the data of {@link StageExecution}.
 */
public interface ExecutionMonitor
{
    /**
     * Signals the start of one or more stages,
     */
    void start( StageExecution[] executions );

    /**
     * Signals the end of the executions previously {@link #start(StageExecution[]) stated}
     */
    void end( StageExecution[] executions, long totalTimeMillis );

    /**
     * Signals the end of the import as a whole
     */
    void done( long totalTimeMillis );

    /**
     * @return next time stamp when this monitor would like to check that status of current execution.
     */
    long nextCheckTime();

    /**
     * Called with currently executing {@link StageExecution} instances so that data from them can be gathered.
     */
    void check( StageExecution[] executions );
}
