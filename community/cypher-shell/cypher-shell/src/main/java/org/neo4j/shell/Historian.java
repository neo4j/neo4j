/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.shell;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.lang.System.getProperty;

/**
 * An object which keeps a record of past commands
 */
public interface Historian
{
    Historian empty = new EmptyHistory();

    /**
     * @return a list of all past commands in the history, in order of execution (first command sorted first).
     */
    List<String> getHistory();

    /**
     * Flush history to disk
     */
    void flushHistory() throws IOException;

    void clear() throws IOException;

    static File defaultHistoryFile()
    {
        // Storing in same directory as driver uses
        File dir = new File( getProperty( "user.home" ), ".neo4j" );
        return new File( dir, ".cypher_shell_history" );
    }

    class EmptyHistory implements Historian
    {
        @Override
        public List<String> getHistory()
        {
            return Collections.emptyList();
        }

        @Override
        public void flushHistory() throws IOException
        {
        }

        @Override
        public void clear() throws IOException
        {
        }
    }
}
