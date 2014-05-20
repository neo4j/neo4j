/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.io.pagecache.impl.legacy;

import java.io.IOException;

import org.neo4j.io.pagecache.impl.common.Page;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.WindowPool;

public class WindowPoolPage implements Page
{
    private final PersistenceWindow window;
    private final Buffer buffer;

    public WindowPoolPage( PersistenceWindow window )
    {
        this.window = window;
        this.buffer = window.getBuffer();
    }

    @Override
    public byte getByte( int offset )
    {
        buffer.setOffset( offset );
        return buffer.get();
    }

    @Override
    public void putByte( byte value, int offset )
    {
        buffer.setOffset( offset );
        buffer.put( value );
    }

    @Override
    public long getLong( int offset )
    {
        buffer.setOffset( offset );
        return buffer.getLong();
    }

    @Override
    public void putLong( long value, int offset )
    {
        buffer.setOffset( offset );
        buffer.putLong( value );
    }

    @Override
    public int getInt( int offset )
    {
        buffer.setOffset( offset );
        return buffer.getInt();
    }

    @Override
    public void putInt( int value, int offset )
    {
        buffer.setOffset( offset );
        buffer.putInt( value );
    }

    @Override
    public void getBytes( byte[] data, int offset )
    {
        buffer.setOffset( offset );
        buffer.get( data );
    }

    @Override
    public void putBytes( byte[] data, int offset )
    {
        buffer.setOffset( offset );
        buffer.put( data );
    }

    public void release( WindowPool pool ) throws IOException
    {
        pool.release( window );
    }
}
