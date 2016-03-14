/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log;

import java.io.IOException;

import org.neo4j.cursor.RawCursor;

public interface RaftLogCursor extends RawCursor<RaftLogEntry,Exception>
{
    @Override
    boolean next() throws IOException, RaftLogCompactedException;

    @Override
    void close() throws IOException;

    static RaftLogCursor empty()
    {
        return new RaftLogCursor()
        {
            @Override
            public boolean next() throws IOException
            {
                return false;
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public RaftLogEntry get()
            {
                throw new IllegalStateException();
            }
        };
    }
}
