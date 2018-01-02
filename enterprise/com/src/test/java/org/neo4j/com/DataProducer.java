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

import static java.lang.Math.min;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class DataProducer implements ReadableByteChannel
{
    private int bytesLeftToProduce;
    private boolean closed;

    public DataProducer( int size )
    {
        this.bytesLeftToProduce = size;
    }
    
    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    @Override
    public void close() throws IOException
    {
        if ( closed )
            throw new IllegalStateException( "Already closed" );
        closed = true;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        int toFill = min( dst.remaining(), bytesLeftToProduce ), leftToFill = toFill;
        if ( toFill <= 0 )
            return -1;
        
        while ( leftToFill-- > 0 )
            dst.put( (byte) 5 );
        bytesLeftToProduce -= toFill;
        return toFill;
    }
}
