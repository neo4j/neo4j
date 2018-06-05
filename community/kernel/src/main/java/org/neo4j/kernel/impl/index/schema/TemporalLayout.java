/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.kernel.impl.index.schema.TemporalSchemaKey.TYPE_DATE;
import static org.neo4j.kernel.impl.index.schema.TemporalSchemaKey.TYPE_DURATION;
import static org.neo4j.kernel.impl.index.schema.TemporalSchemaKey.TYPE_LOCAL_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.TemporalSchemaKey.TYPE_LOCAL_TIME;
import static org.neo4j.kernel.impl.index.schema.TemporalSchemaKey.TYPE_ZONED_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.TemporalSchemaKey.TYPE_ZONED_TIME;

class TemporalLayout extends SchemaLayout<TemporalSchemaKey>
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

    TemporalLayout()
    {
        super( Layout.namedIdentifier( "temp", 9876 ), 1, 0 );
    }

    @Override
    public TemporalSchemaKey newKey()
    {
        return new TemporalSchemaKey();
    }

    @Override
    public TemporalSchemaKey copyKey( TemporalSchemaKey temporalSchemaKey, TemporalSchemaKey key )
    {
        key.copyFrom( temporalSchemaKey );
        return key;
    }

    @Override
    public int keySize( TemporalSchemaKey key )
    {
        return key.size();
    }

    @Override
    public void writeKey( PageCursor cursor, TemporalSchemaKey key )
    {
        cursor.putByte( key.type );
        switch ( key.type )
        {
        case TYPE_DATE:
            cursor.putLong( key.var1 );
            break;
        case TYPE_DURATION:
            cursor.putLong( key.var1 );
            cursor.putInt( (int) key.var2 );
            cursor.putLong( key.var3 );
            cursor.putLong( key.var4 );
            break;
        case TYPE_LOCAL_DATE_TIME:
            cursor.putLong( key.var1 );
            cursor.putInt( (int) key.var2 );
            break;
        case TYPE_LOCAL_TIME:
            cursor.putLong( key.var1 );
            break;
        case TYPE_ZONED_DATE_TIME:
            cursor.putLong( key.var1 );
            cursor.putInt( (int) key.var2 );
            if ( key.var3 >= 0 )
            {
                cursor.putInt( ((int) key.var3) | ZONE_ID_FLAG );
            }
            else
            {
                cursor.putInt( ((int) key.var4) & ZONE_ID_MASK );
            }
            break;
        case TYPE_ZONED_TIME:
            cursor.putLong( key.var1 );
            cursor.putInt( (int) key.var2 );
            break;
        default:
            throw new IllegalArgumentException( "Unexpected type " + key.type );
        }

        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, TemporalSchemaKey key, int keySize )
    {
        key.type = cursor.getByte();
        switch ( key.type )
        {
        case TYPE_DATE:
            key.var1 = cursor.getLong();
            break;
        case TYPE_DURATION:
            key.var1 = cursor.getLong();
            key.var2 = cursor.getInt();
            key.var3 = cursor.getLong();
            key.var4 = cursor.getLong();
            break;
        case TYPE_LOCAL_DATE_TIME:
            key.var1 = cursor.getLong();
            key.var2 = cursor.getInt();
            break;
        case TYPE_LOCAL_TIME:
            key.var1 = cursor.getLong();
            break;
        case TYPE_ZONED_DATE_TIME:
            key.var1 = cursor.getLong();
            key.var2 = cursor.getInt();
            int encodedZone = cursor.getInt();
            if ( isZoneId( encodedZone ) )
            {
                key.var3 = asZoneId( encodedZone );
                key.var4 = 0;
            }
            else
            {
                key.var3 = -1;
                key.var4 = asZoneOffset( encodedZone );
            }
            break;
        case TYPE_ZONED_TIME:
            key.var1 = cursor.getLong();
            key.var2 = cursor.getInt();
            break;
        default:
            // could be a bad read
        }

        key.setEntityId( cursor.getLong() );
    }

    private short asZoneId( int encodedZone )
    {
        return (short) ( encodedZone & ZONE_ID_MASK );
    }

    private boolean isZoneId( int encodedZone )
    {
        return ( encodedZone & ZONE_ID_FLAG ) != 0;
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

    @Override
    public boolean fixedSize()
    {
        return false;
    }
}
