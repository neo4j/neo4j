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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

/**
 * Used to check if a log rotation is needed, and also to execute a log rotation.
 *
 * The implementation also makes sure that stores are forced to disk.
 *
 */
public interface LogRotation
{
    public interface Monitor
    {
        void rotatedLog();
    }

    public static LogRotation NO_ROTATION = new LogRotation()
    {
        @Override
        public boolean rotateLogIfNeeded() throws IOException
        {
            return false;
        }

        @Override
        public void rotateLogFile() throws IOException
        {

        }
    };

    /**
     * @return {@code true} if a rotation is needed, and performs one if needed.
     */
    boolean rotateLogIfNeeded() throws IOException;

    /**
     * Force a log rotation.
     *
     * @throws IOException
     */
    void rotateLogFile() throws IOException;
}
