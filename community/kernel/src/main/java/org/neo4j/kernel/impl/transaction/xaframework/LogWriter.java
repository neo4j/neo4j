/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

import org.neo4j.kernel.configuration.Config;

public interface LogWriter
{
    /**
     * Create a new active log file (a file that will be picked up and used for recovery), and return
     * a log buffer that allows writing to the file. The resulting file should contain any necessary
     * headers and so on, and allow directly appending new transactions.
     *
     * @throws IllegalStateException if an active file already exists at the specified location
     */
    LogBuffer createActiveLogFile( Config config, long prevCommittedId ) throws IllegalStateException, IOException;

}
