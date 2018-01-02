/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * This will produce data like (bytes):
 * 
 * 0,1,2,3,4,5,6,7,8,9,0,1,2,3,4... a.s.o.
 * 
 * Up until {@code size} number of bytes has been returned.
 * 
 */
public class KnownDataByteChannel implements ReadableByteChannel
{
    protected int position;
    private final int size;
    
    public KnownDataByteChannel( int size )
    {
        this.size = size;
    }
    
    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        int toRead = Math.min( dst.limit()-dst.position(), left() );
        if ( toRead == 0 )
        {
            return -1;
        }
        
        for ( int i = 0; i < toRead; i++ )
        {
            dst.put( (byte)((position++)%10) );
        }
        return toRead;
    }

    private int left()
    {
        return size-position;
    }
}
