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

package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.stream.ChunkedInput;

public abstract class ChunkedData implements ChunkedInput
{
    private ChannelBuffer chunk;

    public boolean hasNextChunk()
    {
        if ( chunk != null ) return true;
        chunk = writeNextChunk();
        if ( chunk == null || !chunk.readable() )
        { // No more data!
            chunk = null;
            close(); // we are done!
            return false;
        }
        else
        {
            return true;
        }
    }

    public final ChannelBuffer nextChunk()
    {
        if ( hasNextChunk() )
        {
            try
            {
                return chunk;
            }
            finally
            {
                chunk = null;
            }
        }
        else
        {
            return null;
        }
    }

    public final boolean isEndOfInput()
    {
        return !hasNextChunk();
    }

    protected abstract ChannelBuffer writeNextChunk();

    public abstract void close();
}
