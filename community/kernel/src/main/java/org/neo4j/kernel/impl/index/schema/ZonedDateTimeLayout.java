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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for absolute date times.
 */
class ZonedDateTimeLayout extends SchemaLayout<ZonedDateTimeSchemaKey>
{
    // A 1 signals a named time zone is stored, a 0 that an offset is stored
    private static final int ZONE_ID_FLAG = 0x0100_0000;
    // Mask for offsets to remove to not collide with the flag for negative numbers
    // It is 24 bits which allows to store all possible minute offsets
    private static final int ZONE_ID_MASK = 0x00FF_FFFF;
    // This is used to determine if the value is negative (after applying the bitmask)
    private static final int ZONE_ID_HIGH = 0x0080_0000;
    // This is ised to restore masked negative offsets to their real value
    private static final int ZONE_ID_EXT =  0xFF00_0000;

    ZonedDateTimeLayout()
    {
        super( "Tdt", 0, 1 );
    }

    @Override
    public ZonedDateTimeSchemaKey newKey()
    {
        return new ZonedDateTimeSchemaKey();
    }

    @Override
    public ZonedDateTimeSchemaKey copyKey( ZonedDateTimeSchemaKey key, ZonedDateTimeSchemaKey into )
    {
        into.epochSecondUTC = key.epochSecondUTC;
        into.nanoOfSecond = key.nanoOfSecond;
        into.zoneId = key.zoneId;
        into.zoneOffsetSeconds = key.zoneOffsetSeconds;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public int keySize( ZonedDateTimeSchemaKey key )
    {
        return ZonedDateTimeSchemaKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, ZonedDateTimeSchemaKey key )
    {
        cursor.putLong( key.epochSecondUTC );
        cursor.putInt( key.nanoOfSecond );
        if ( key.zoneId >= 0 )
        {
            cursor.putInt( key.zoneId | ZONE_ID_FLAG );
        }
        else
        {
            cursor.putInt( key.zoneOffsetSeconds & ZONE_ID_MASK );
        }
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, ZonedDateTimeSchemaKey into, int keySize )
    {
        into.epochSecondUTC = cursor.getLong();
        into.nanoOfSecond = cursor.getInt();
        int encodedZone = cursor.getInt();
        if ( isZoneId( encodedZone ) )
        {
            into.zoneId = asZoneId( encodedZone );
            into.zoneOffsetSeconds = 0;
        }
        else
        {
            into.zoneId = -1;
            into.zoneOffsetSeconds = asZoneOffset( encodedZone );
        }
        into.setEntityId( cursor.getLong() );
    }

    private int asZoneOffset( int encodedZone )
    {
        if ( (ZONE_ID_HIGH & encodedZone) == ZONE_ID_HIGH )
        {
            return ZONE_ID_EXT | encodedZone;
        }
        else
        {
            return encodedZone;
        }
    }

    private short asZoneId( int encodedZone )
    {
        return (short) ( encodedZone & ZONE_ID_MASK );
    }

    private boolean isZoneId( int encodedZone )
    {
        return ( encodedZone & ZONE_ID_FLAG ) != 0;
    }
}
