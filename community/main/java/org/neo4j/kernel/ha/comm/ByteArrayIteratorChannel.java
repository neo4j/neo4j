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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

class ByteArrayIteratorChannel implements ReadableByteChannel
{
    private int pos = 0;
    private byte[] current;
    private Iterator<byte[]> iterator;

    ByteArrayIteratorChannel( Iterator<byte[]> iterator )
    {
        if ( iterator.hasNext() )
        {
            this.current = iterator.next();
        }
        else
        {
            this.current = new byte[0];
        }
        this.iterator = iterator;
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        if ( pos >= current.length && !iterator.hasNext() ) return -1;
        int size = 0;
        while ( dst.hasRemaining() && ( pos < current.length || iterator.hasNext() ) )
        {
            if ( pos < current.length )
            {
                current = iterator.next();
                pos = 0;
            }
            int length = Math.min( current.length - pos, dst.limit() - dst.position() );
            dst.put( current, pos, length );
            pos += length;
            size += length;
        }
        return size;
    }

    public void close() throws IOException
    {
        current = null;
        iterator = null;
    }

    public boolean isOpen()
    {
        return current != null;
    }

}
