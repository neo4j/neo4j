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
package org.neo4j.internal.store.cursors;

public abstract class ReadCursor extends MemoryAccess implements AutoCloseable
{
    public final boolean shouldRetry()
    {
        long token = pageMan.refreshLockToken( pageId, base, offset, lockToken );
        if ( token != lockToken )
        {
            lockToken = token;
            return true;
        }
        else
        {
            return false;
        }
    }

    protected final long virtualAddress()
    {
        return virtualAddress;
    }

    /**
     * @param virtualAddress the virtual address to go to
     * @return {@code true} if the virtual address is valid, {@code false} otherwise.
     */
    protected final boolean moveToVirtualAddress( long virtualAddress )
    {
        if ( pageMan == null )
        {
            throw new IllegalStateException( "Cursor has not been initialized." );
        }
        return pageMan.moveToVirtualAddress( virtualAddress, this, pageId, base, offset, lockToken );
    }

    protected void initializeScanCursor()
    {
        virtualAddress -= 1;
    }

    protected boolean scanNextByVirtualAddress( long maxAddress )
    {
        for ( long address = virtualAddress; ; address = virtualAddress )
        {
            if ( address < maxAddress )
            {
                if ( moveToVirtualAddress( address + 1 ) )
                {
                    return true;
                }
            }
            else
            {
                return false;
            }
        }
    }

    @Override
    public final void close()
    {
        try
        {
            closeImpl();
        }
        finally
        {
            closeAccess();
        }
    }

    protected void closeImpl()
    {
        // default: do nothing. Allow override in subclasses
    }

    // TODO: perhaps all dynamically sized reads should ALWAYS verify the bounds?

    protected final byte readByte( int offset )
    {
        return Memory.getByte( address( offset, 1 ) );
    }

    protected final int unsignedByte( int offset )
    {
        return 0xFF & readByte( offset );
    }

    protected final void read( int offset, byte[] target, int pos, int length )
    {
        Memory.copyToArray( address( offset, length ), target, pos, length );
    }

    protected final short readShort( int offset )
    {
        return Memory.getShort( address( offset, 2 ) );
    }

    protected final int unsignedShort( int offset )
    {
        return 0xFFFF & readShort( offset );
    }

    protected final void read( int offset, short[] target, int pos, int length )
    {
        Memory.copyToArray( address( offset, length * 2 ), target, pos, length );
    }

    protected final int readInt( int offset )
    {
        return Memory.getInt( address( offset, 4 ) );
    }

    protected final long unsignedInt( int offset )
    {
        return 0xFFFF_FFFFL & readInt( offset );
    }

    protected final void read( int offset, int[] target, int pos, int length )
    {
        Memory.copyToArray( address( offset, length * 4 ), target, pos, length );
    }

    protected final long readLong( int offset )
    {
        return Memory.getLong( address( offset, 8 ) );
    }

    protected final void read( int offset, long[] target, int pos, int length )
    {
        Memory.copyToArray( address( offset, length * 8 ), target, pos, length );
    }

    protected final char readChar( int offset )
    {
        return Memory.getChar( address( offset, 2 ) );
    }

    protected final void read( int offset, char[] target, int pos, int length )
    {
        Memory.copyToArray( address( offset, length * 2 ), target, pos, length );
    }

    protected final void read( int offset, float[] target, int pos, int length )
    {
        Memory.copyToArray( address( offset, length * 4 ), target, pos, length );
    }

    protected final void read( int offset, double[] target, int pos, int length )
    {
        Memory.copyToArray( address( offset, length * 8 ), target, pos, length );
    }

    /**
     * Copy the data content of this cursor to the given target cursor. Note that the caller is responsible for making
     * sure that the target cursor is properly initialized and positioned.
     *
     * @param target
     *         the cursor to copy the data to.
     */
    protected final void transferTo( Writer target )
    {
        target.receive( 0, address( 0, dataBound() ), dataBound() );
    }

    protected final void transferTo( int offset, Writer target, int targetOffset, int bytes )
    {
        target.receive( targetOffset, address( offset, bytes ), bytes );
    }
}
