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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.Closeable;
import java.io.IOException;

import static org.neo4j.kernel.impl.store.kvstore.BigEndianByteArrayBuffer.newBuffer;

class KeyValueMerger implements DataProvider
{
    private final DataProvider first;
    private final DataProvider other;
    // We copy from the two sources to these extra buffers in order to compare the keys,
    // is there any way we could avoid this extra buffering?
    private final BigEndianByteArrayBuffer firstKey;
    private final BigEndianByteArrayBuffer firstValue;
    private final BigEndianByteArrayBuffer otherKey;
    private final BigEndianByteArrayBuffer otherValue;
    private boolean firstAvail;
    private boolean otherAvail;

    KeyValueMerger( DataProvider first, DataProvider other, int keySize, int valueSize ) throws IOException
    {
        this.firstAvail = (this.first = first).visit( firstKey = newBuffer( keySize ), firstValue = newBuffer( valueSize ) );
        this.otherAvail = (this.other = other).visit( otherKey = newBuffer( keySize ), otherValue = newBuffer( valueSize ) );
    }

    @Override
    public boolean visit( WritableBuffer key, WritableBuffer value ) throws IOException
    {
        if ( firstAvail && otherAvail )
        {
            int cmp = firstKey.compareTo( otherKey.buffer );
            if ( cmp < 0 )
            {
                firstKey.read( key );
                firstValue.read( value );
                firstAvail = first.visit( firstKey, firstValue );
            }
            else
            {
                otherKey.read( key );
                otherValue.read( value );
                otherAvail = other.visit( otherKey, otherValue );
                if ( cmp == 0 )
                {
                    firstAvail = first.visit( firstKey, firstValue );
                }
            }
            return true;
        }
        else if ( firstAvail )
        {
            firstKey.read( key );
            firstValue.read( value );
            firstAvail = first.visit( firstKey, firstValue );
            return true;
        }
        else if ( otherAvail )
        {
            otherKey.read( key );
            otherValue.read( value );
            otherAvail = other.visit( otherKey, otherValue );
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        try ( Closeable ignored = other )
        {
            first.close();
        }
    }
}
