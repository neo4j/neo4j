/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * An object which keeps a record of past commands
 */
public interface Historian
{
    Historian empty = new Historian()
    {
        @Nonnull
        @Override
        public List<String> getHistory()
        {
            return new ArrayList<>();
        }

        @Override
        public void flushHistory()
        {
        }
    };

    /**
     * @return a list of all past commands in the history, in order of execution (first command sorted first).
     */
    @Nonnull
    List<String> getHistory();

    /**
     * Flush history to disk
     */
    void flushHistory() throws IOException;
}
