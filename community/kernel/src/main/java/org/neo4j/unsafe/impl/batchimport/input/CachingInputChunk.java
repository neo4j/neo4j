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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;

class CachingInputChunk implements InputChunk
{
    private final InputChunk actual;
    private final InputCacher cacher;
    private InputEntityVisitor wrapped;
    private InputEntityVisitor unwrapped;

    CachingInputChunk( InputChunk actual, InputCacher cacher )
    {
        this.actual = actual;
        this.cacher = cacher;
    }

    InputChunk actual()
    {
        return actual;
    }

    @Override
    public void close() throws IOException
    {
        actual.close();
    }

    @Override
    public boolean next( InputEntityVisitor unwrapped ) throws IOException
    {
        if ( wrapped == null )
        {
            this.unwrapped = unwrapped;
            wrapped = cacher.wrap( unwrapped );
        }
        else
        {
            assert this.unwrapped == unwrapped;
        }
        return actual.next( wrapped );
    }
}
