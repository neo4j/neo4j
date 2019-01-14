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
package org.neo4j.diagnostics;

/**
 * Interface for handling feedback to the user. Implementations of this should be responsible of presenting the progress
 * to the user. Some specialised implementations can choose to omit any of the information provided here.
 */
public interface DiagnosticsReporterProgress
{
    /**
     * Calling this will notify the user that the percentage has changed.
     *
     * @param percent to display to the user.
     */
    void percentChanged( int percent );

    /**
     * Adds an additional information string to the output. Useful if the task has multiple steeps and the current step
     * should be displayed.
     *
     * @param info string to present to the user.
     */
    void info( String info );

    /**
     * Called if an internal error occurs with an optional exception.
     *
     * @param msg message to display to the user.
     * @param throwable optional exception, used to include a stacktrace if applicable.
     */
    void error( String msg, Throwable throwable );

    /**
     * @apiNote Called by dispatching class. Should not be called from diagnostics sources.
     */
    void setTotalSteps( long steps );

    /**
     * @apiNote Called by dispatching class. Should not be called from diagnostics sources.
     */
    void started( long currentStepIndex, String target );

    /**
     * @apiNote Called by dispatching class. Should not be called from diagnostics sources.
     */
    void finished();
}
