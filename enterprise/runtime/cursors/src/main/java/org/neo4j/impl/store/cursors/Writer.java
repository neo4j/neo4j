/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.impl.store.cursors;

public abstract class Writer extends MemoryAccess implements AutoCloseable
{
    @Override
    protected void closeImpl()
    {
        // default: do nothing. Allow override in subclasses
    }

    // TODO: perhaps all dynamically sized writes should ALWAYS verify the bounds?

    protected final void writeByte( int offset, byte value )
    {
        Memory.putByte( address( offset, 1 ), value );
    }

    protected final void write( int offset, byte[] source, int pos, int length )
    {
        Memory.copyFromArray( address( offset, length ), source, pos, length );
    }

    protected final void writeShort( int offset, short value )
    {
        Memory.putShort( address( offset, 2 ), value );
    }

    protected final void write( int offset, short[] source, int pos, int length )
    {
        Memory.copyFromArray( address( offset, length * 2 ), source, pos, length );
    }

    protected final void writeInt( int offset, int value )
    {
        Memory.putInt( address( offset, 4 ), value );
    }

    protected final void write( int offset, int[] source, int pos, int length )
    {
        Memory.copyFromArray( address( offset, length * 4 ), source, pos, length );
    }

    protected final void writeLong( int offset, long value )
    {
        Memory.putLong( address( offset, 8 ), value );
    }

    protected final void write( int offset, long[] source, int pos, int length )
    {
        Memory.copyFromArray( address( offset, length * 8 ), source, pos, length );
    }

    protected final void writeChar( int offset, char value )
    {
        Memory.putChar( address( offset, 2 ), value );
    }

    protected final void write( int offset, char[] source, int pos, int length )
    {
        Memory.copyFromArray( address( offset, length * 2 ), source, pos, length );
    }

    protected final void write( int offset, float[] source, int pos, int length )
    {
        Memory.copyFromArray( address( offset, length * 4 ), source, pos, length );
    }

    protected final void write( int offset, double[] source, int pos, int length )
    {
        Memory.copyFromArray( address( offset, length * 8 ), source, pos, length );
    }

    protected void fill( int offset, int size, byte data )
    {
        Memory.fill( address( offset, size ), size, data );
    }

    void receive( int offset, long sourceAddress, int size )
    {
        Memory.copy( sourceAddress, address( offset, size ), size );
    }
}
