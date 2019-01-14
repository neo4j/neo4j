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
package org.neo4j.bolt.v1.messaging.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ArrayByteChannel implements ReadableByteChannel
{
    private final ByteBuffer data;

    public ArrayByteChannel( byte[] bytes )
    {
        this.data = ByteBuffer.wrap( bytes );
    }

    @Override
    public int read( ByteBuffer dst )
    {
        if ( data.position() == data.limit() )
        {
            return -1;
        }
        int originalPosition = data.position();
        int originalLimit = data.limit();
        data.limit( Math.min( data.limit(), dst.limit() - dst.position() + data.position() ) );
        dst.put( data );
        data.limit( originalLimit );
        return data.position() - originalPosition;
    }

    @Override
    public boolean isOpen()
    {
        return data.position() < data.limit();
    }

    @Override
    public void close()
    {
    }
}
