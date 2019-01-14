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

import java.io.IOException;
import java.nio.file.Path;

/**
 * A diagnostic source is a pair of a destination and a function to add data to mentioned destination.
 * The destination has to be provided separately since the creation of the file is done outside of the method to
 * make it more flexible in regards to where the file can be placed.
 */
public interface DiagnosticsReportSource
{
    /**
     * The final path of the diagnostic source, it is relative to the archive base directory.
     *
     * @return a path as a string representation.
     */
    String destinationPath();

    /**
     * This method should output the diagnostics source to the provided destination.
     *
     * @param archiveDestination the target destination that should be written to.
     * @param progress a monitor that can track progress.
     * @throws IOException if any file operations fail, exceptions should be handled by the caller for better error
     * reporting to the user.
     */
    void addToArchive( Path archiveDestination, DiagnosticsReporterProgress progress ) throws IOException;

    /**
     * Returns an estimated upper bound of the input file size. Since the content will be placed in an archive the final
     * size can actually both increase and decrease.
     *
     * @param progress a monitor that can track progress.
     * @return the estimated file size in bytes.
     * @throws IOException if size cannot be determined.
     */
    long estimatedSize( DiagnosticsReporterProgress progress ) throws IOException;
}
