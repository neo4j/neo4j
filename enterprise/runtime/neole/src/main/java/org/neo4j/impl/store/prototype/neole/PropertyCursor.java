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
package org.neo4j.impl.store.prototype.neole;

import org.neo4j.values.Value;
import org.neo4j.values.ValueGroup;
import org.neo4j.values.ValueWriter;
import org.neo4j.values.Values;

class PropertyCursor extends org.neo4j.impl.store.prototype.PropertyCursor<ReadStore>
{
    /**
     * <pre>
     *  0: high bits  ( 1 byte)
     *  1: next       ( 4 bytes)
     *  5: prev       ( 4 bytes)
     *  9: payload    (32 bytes - 4 x 8 byte blocks)
     * </pre>
     * <h2>high bits</h2>
     * <pre>
     * [    ,xxxx] high(next)
     * [xxxx,    ] high(prev)
     * </pre>
     * <h2>block structure</h2>
     * <pre>
     * [][][][] [    ,xxxx] [    ,    ] [    ,    ] [    ,    ] type (0x0000_0000_0F00_0000)
     * [][][][] [    ,    ] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] key  (0x0000_0000_00FF_FFFF)
     * </pre>
     * <h2>property types</h2>
     * <pre>
     *  1: BOOL
     *  2: BYTE
     *  3: SHORT
     *  4: CHAR
     *  5: INT
     *  6: LONG
     *  7: FLOAT
     *  8: DOUBLE
     *  9: STRING REFERENCE
     * 10: ARRAY  REFERENCE
     * 11: SHORT STRING
     * 12: SHORT ARRAY
     * </pre>
     * <h2>value formats</h2>
     * <pre>
     * BOOL:      [    ,    ] [    ,    ] [    ,    ] [    ,    ] [   x,type][K][K][K]           (0x0000_0000_1000_0000)
     * BYTE:      [    ,    ] [    ,    ] [    ,    ] [    ,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_000F_F000_0000)
     * SHORT:     [    ,    ] [    ,    ] [    ,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_0FFF_F000_0000)
     * CHAR:      [    ,    ] [    ,    ] [    ,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0000_0FFF_F000_0000)
     * INT:       [    ,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0FFF_FFFF_F000_0000)
     * LONG:      [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxx1,type][K][K][K] inline>>29(0xFFFF_FFFF_7000_0000)
     * LONG:      [    ,    ] [    ,    ] [    ,    ] [    ,    ] [   0,type][K][K][K] value in next long block
     * FLOAT:     [    ,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0x0FFF_FFFF_F000_0000)
     * DOUBLE:    [    ,    ] [    ,    ] [    ,    ] [    ,    ] [    ,type][K][K][K] value in next long block
     * REFERENCE: [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [xxxx,type][K][K][K]    (>>28) (0xFFFF_FFFF_F000_0000)
     * SHORT STR: [    ,    ] [    ,    ] [    ,    ] [    ,   x] [xxxx,type][K][K][K] encoding  (0x0000_0001_F000_0000)
     *            [    ,    ] [    ,    ] [    ,    ] [ xxx,xxx ] [    ,type][K][K][K] length    (0x0000_007E_0000_0000)
     *            [xxxx,xxxx] [xxxx,xxxx] [xxxx,xxxx] [x   ,    ] payload(+ maybe in next block) (0xFFFF_FF80_0000_0000)
     *                                                            bits are densely packed, bytes torn across blocks
     * SHORT ARR: [    ,    ] [    ,    ] [    ,    ] [    ,    ] [xxxx,type][K][K][K] data type (0x0000_0000_F000_0000)
     *            [    ,    ] [    ,    ] [    ,    ] [  xx,xxxx] [    ,type][K][K][K] length    (0x0000_003F_0000_0000)
     *            [    ,    ] [    ,    ] [    ,xxxx] [xx  ,    ] [    ,type][K][K][K] bits/item (0x0000_003F_0000_0000)
     *                                                                                 0 means 64, other values "normal"
     *            [xxxx,xxxx] [xxxx,xxxx] [xxxx,    ] [    ,    ] payload(+ maybe in next block) (0xFFFF_FF00_0000_0000)
     *                                                            bits are densely packed, bytes torn across blocks
     * </pre>
     */
    static final int RECORD_SIZE = 41;
    static final int BOOL = 1, BYTE = 2, SHORT = 3, CHAR = 4, INT = 5, LONG = 6, FLOAT = 7, DOUBLE = 8,
            STRING_REFERENCE = 9, ARRAY_REFERENCE = 10, SHORT_STRING = 11, SHORT_ARRAY = 12;
    private final ByteBlockCursor bytes;
    private int block;

    PropertyCursor( ReadStore store, ByteBlockCursor bytes )
    {
        super( store );
        this.bytes = bytes;
        block = Integer.MIN_VALUE;
    }

    @Override
    public boolean next()
    {
        while ( block >= -1 && block < 3 )
        {
            block++;
            if ( typeIdentifier() != 0 )
            {
                return true;
            }
        }
        // TODO: move to next record if needed
        close();
        return false;
    }

    @Override
    protected void closeImpl()
    {
        block = Integer.MIN_VALUE;
    }

    @Override
    public int propertyKey()
    {
        return (int) (block( block ) & 0x00FF_FFFFL);
    }

    int typeIdentifier()
    {
        return (int) ((block( block ) & 0x0F00_0000L) >> 24);
    }

    private long block( int offset )
    {
        return readLong( 9 + Long.BYTES * offset );
    }

    @Override
    public ValueGroup propertyType()
    {
        switch ( typeIdentifier() )
        {
        case BOOL:
            return ValueGroup.BOOLEAN;
        case BYTE:
            return ValueGroup.NUMBER;
        case SHORT:
            return ValueGroup.NUMBER;
        case CHAR:
            return ValueGroup.TEXT;
        case INT:
            return ValueGroup.NUMBER;
        case LONG:
            return ValueGroup.NUMBER;
        case FLOAT:
            return ValueGroup.NUMBER;
        case DOUBLE:
            return ValueGroup.NUMBER;
        case STRING_REFERENCE:
            return ValueGroup.TEXT;
        case ARRAY_REFERENCE:
            throw new UnsupportedOperationException( "not implemented" );
        case SHORT_STRING:
            return ValueGroup.TEXT;
        case SHORT_ARRAY:
            throw new UnsupportedOperationException( "not implemented" );
        default:
            return null;
        }
    }

    @Override
    public Value propertyValue()
    {
        switch ( typeIdentifier() )
        {
        case BOOL:
//            return (block(block) & 0x0000_0000_1000_0000) != 0;
            throw new UnsupportedOperationException( "not implemented" );
        case BYTE:
            throw new UnsupportedOperationException( "not implemented" );
        case SHORT:
            throw new UnsupportedOperationException( "not implemented" );
        case CHAR:
            throw new UnsupportedOperationException( "not implemented" );
        case INT:
            return Values.intValue( (int)(block( block ) & 0x0FFF_FFFF_F000_0000L) >> 28 );
        case LONG:
            throw new UnsupportedOperationException( "not implemented" );
        case FLOAT:
            throw new UnsupportedOperationException( "not implemented" );
        case DOUBLE:
            throw new UnsupportedOperationException( "not implemented" );
        case STRING_REFERENCE:
            throw new UnsupportedOperationException( "not implemented" );
        case ARRAY_REFERENCE:
            throw new UnsupportedOperationException( "not implemented" );
        case SHORT_STRING:
            throw new UnsupportedOperationException( "not implemented" );
        case SHORT_ARRAY:
            throw new UnsupportedOperationException( "not implemented" );
        default:
            return null;
        }
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> target )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    protected int dataBound()
    {
        return RECORD_SIZE;
    }

    void init( StoreFile properties, long reference )
    {
        ReadStore.setup( properties, this, reference );
        block = -1;
    }
}
