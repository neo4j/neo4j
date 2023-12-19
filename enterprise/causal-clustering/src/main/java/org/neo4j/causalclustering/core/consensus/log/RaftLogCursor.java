/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

import org.neo4j.cursor.RawCursor;

public interface RaftLogCursor extends RawCursor<RaftLogEntry,Exception>
{
    @Override
    boolean next() throws IOException;

    @Override
    void close() throws IOException;

    long index();

    static RaftLogCursor empty()
    {
        return new RaftLogCursor()
        {
            @Override
            public boolean next()
            {
                return false;
            }

            @Override
            public void close()
            {
            }

            @Override
            public long index()
            {
                return -1;
            }

            @Override
            public RaftLogEntry get()
            {
                throw new IllegalStateException();
            }
        };
    }
}
