/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.onlinebackup;

import java.io.IOException;
import java.util.logging.FileHandler;

/**
 * Online backup for Neo4j.
 */
public interface Backup
{
    /**
     * Perform the backup.
     */
    void doBackup() throws IOException;

    /**
     * Enable logging to file. The log messages will be appended to the
     * backup.log file in the current working directory.
     */
    void enableFileLogger() throws SecurityException, IOException;

    /**
     * Enable logging to the specified file.
     * 
     * @param filename file name of log file
     */
    void enableFileLogger( String filename ) throws SecurityException,
            IOException;

    /**
     * Enable a user-provided {@link FileHandler}
     * 
     * @param handler file handler for logging
     */
    void enableFileLogger( FileHandler handler );

    /**
     * Disable logging to file.
     */
    void disableFileLogger();

    /**
     * Enable debug logging. Adds debug output to both console and file (if file
     * output is enabled).
     */
    void setLogLevelDebug();

    /**
     * Set logging to normal. Changes settings for both console and file output.
     */
    void setLogLevelNormal();

    /**
     * Turn off all logging.
     */
    void setLogLevelOff();
}
