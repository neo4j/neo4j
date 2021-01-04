/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for {@link GBPTree} used by {@link NativeTokenScanStore}.
 *
 * <ul>
 * <li>
 * Each keys is a combination of {@code tokenId} and {@code entityIdRange} ({@code entityId/64}).
 * </li>
 * <li>
 * Each value is a 64-bit bit set (a primitive {@code long}) where each set bit in it represents
 * a entity with that token, such that {@code entityId = entityIdRange+bitOffset}. Range size is 64 bits.
 * </li>
 * </ul>
 */
public class TokenScanLayout extends Layout.Adapter<TokenScanKey,TokenScanValue>
{
    public TokenScanLayout()
    {
        super( true, Layout.namedIdentifier( IDENTIFIER_NAME, TokenScanValue.RANGE_SIZE ), 0, 1 );
    }

    /**
     * Name part of the {@link #identifier()} value.
     */
    private static final String IDENTIFIER_NAME = "LSL";

    /**
     * Size of each {@link TokenScanKey}.
     */
    private static final int KEY_SIZE = Integer.BYTES/*tokenId*/ + 6/*idRange*/;

    /**
     * Compares {@link TokenScanKey}, giving ascending order of {@code tokenId} then {@code entityIdRange}.
     */
    @Override
    public int compare( TokenScanKey o1, TokenScanKey o2 )
    {
        int tokenComparison = Integer.compare( o1.tokenId, o2.tokenId );
        return tokenComparison != 0 ? tokenComparison : Long.compare( o1.idRange, o2.idRange );
    }

    @Override
    public TokenScanKey newKey()
    {
        return new TokenScanKey();
    }

    @Override
    public TokenScanKey copyKey( TokenScanKey key, TokenScanKey into )
    {
        into.tokenId = key.tokenId;
        into.idRange = key.idRange;
        return into;
    }

    @Override
    public TokenScanValue newValue()
    {
        return new TokenScanValue();
    }

    @Override
    public int keySize( TokenScanKey key )
    {
        return KEY_SIZE;
    }

    @Override
    public int valueSize( TokenScanValue value )
    {
        return TokenScanValue.RANGE_SIZE_BYTES;
    }

    @Override
    public void writeKey( PageCursor cursor, TokenScanKey key )
    {
        cursor.putInt( key.tokenId );
        put6ByteLong( cursor, key.idRange );
    }

    private static void put6ByteLong( PageCursor cursor, long value )
    {
        cursor.putInt( (int) value );
        cursor.putShort( (short) (value >>> Integer.SIZE) );
    }

    @Override
    public void writeValue( PageCursor cursor, TokenScanValue value )
    {
        cursor.putLong( value.bits );
    }

    @Override
    public void readKey( PageCursor cursor, TokenScanKey into, int keySize )
    {
        into.tokenId = cursor.getInt();
        into.idRange = get6ByteLong( cursor );
    }

    private static long get6ByteLong( PageCursor cursor )
    {
        long low4b = cursor.getInt() & 0xFFFFFFFFL;
        long high2b = cursor.getShort();
        return low4b | (high2b << Integer.SIZE);
    }

    @Override
    public void readValue( PageCursor cursor, TokenScanValue into, int valueSize )
    {
        into.bits = cursor.getLong();
    }

    @Override
    public void initializeAsLowest( TokenScanKey key )
    {
        key.set( Integer.MIN_VALUE, Long.MIN_VALUE );
    }

    @Override
    public void initializeAsHighest( TokenScanKey key )
    {
        key.set( Integer.MAX_VALUE, Long.MAX_VALUE );
    }
}
