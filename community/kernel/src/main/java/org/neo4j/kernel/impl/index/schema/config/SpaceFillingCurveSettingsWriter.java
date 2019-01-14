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
package org.neo4j.kernel.impl.index.schema.config;

import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * {@link GBPTree} header writer for {@link SpaceFillingCurveSettings}.
 *
 * @see SpaceFillingCurveSettingsReader
 */
public class SpaceFillingCurveSettingsWriter implements Consumer<PageCursor>
{
    static final byte VERSION = 0;

    /**
     * Biggest theoretical size of a stored setting.
     */
    private static final int WORST_CASE_SETTINGS_SIZE =
            Byte.BYTES +          // tableId
            Integer.BYTES +       // code
            Integer.BYTES +       // maxLevels
            Integer.BYTES +       // dimensions
            (Long.BYTES * 2) * 3; // two doubles per dimension (max 3 dimensions)

    private final IndexSpecificSpaceFillingCurveSettingsCache settings;

    public SpaceFillingCurveSettingsWriter( IndexSpecificSpaceFillingCurveSettingsCache settings )
    {
        this.settings = settings;
    }

    /**
     * Given the {@link PageCursor} goes through all {@link SpaceFillingCurveSettings} that are in use in specific index.
     * The internal {@link IndexSpecificSpaceFillingCurveSettingsCache} provides those settings, which have been collected
     * from geometry values from updates.
     *
     * @param cursor {@link PageCursor} to write index-specific {@link SpaceFillingCurveSettings} into.
     */
    @Override
    public void accept( PageCursor cursor )
    {
        cursor.putByte( VERSION );
        settings.visitIndexSpecificSettings( new IndexSpecificSpaceFillingCurveSettingsCache.SettingVisitor()
        {
            @Override
            public void count( int count )
            {
                cursor.putInt( count );
            }

            @Override
            public void visit( CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings )
            {
                // For tableId+code the native layout is even stricter here, but it'd add unnecessary complexity to shave off a couple of more bits
                cursor.putByte( (byte) assertInt( "table id", crs.getTable().getTableId(), 0xFF ) );
                cursor.putInt( crs.getCode() );
                cursor.putShort( (short) assertInt( "max levels", settings.maxLevels, 0xFFFF ) );
                cursor.putShort( (short) assertInt( "dimensions", settings.dimensions, 0xFFFF ) );
                double[] min = settings.extents.getMin();
                double[] max = settings.extents.getMax();
                for ( int i = 0; i < settings.dimensions; i++ )
                {
                    cursor.putLong( Double.doubleToLongBits( min[i] ) );
                    cursor.putLong( Double.doubleToLongBits( max[i] ) );
                }
            }

            private int assertInt( String name, int value, int mask )
            {
                if ( (value & ~mask) != 0 )
                {
                    throw new IllegalArgumentException( "Invalid " + name + " " + value + ", max is " + mask );
                }
                return value;
            }
        } );
    }

    /**
     * Calculates max number of crs settings that can fit on a tree-state page, given {@code pageSize}.
     *
     * @param pageSize page size in the page cache.
     * @return max number of crs settings a {@link GBPTree} tree-state page can hold given the {@code pageSize}.
     */
    public static int maxNumberOfSettings( int pageSize )
    {
        return (pageSize - Header.OVERHEAD - Long.BYTES/*some settings count/version overhead*/) / WORST_CASE_SETTINGS_SIZE;
    }
}
