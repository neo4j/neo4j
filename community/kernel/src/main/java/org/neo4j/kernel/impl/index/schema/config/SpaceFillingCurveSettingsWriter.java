/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;

public class SpaceFillingCurveSettingsWriter implements Consumer<PageCursor>
{
    private final IndexSpecificSpaceFillingCurveSettingsCache settings;

    public SpaceFillingCurveSettingsWriter( IndexSpecificSpaceFillingCurveSettingsCache settings )
    {
        this.settings = settings;
    }

    @Override
    public void accept( PageCursor cursor )
    {
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
                cursor.putInt( crs.getTable().getTableId() );
                cursor.putInt( crs.getCode() );

                cursor.putInt( settings.maxLevels );
                cursor.putInt( settings.dimensions );
                double[] min = settings.extents.getMin();
                double[] max = settings.extents.getMax();
                for ( int i = 0; i < settings.dimensions; i++ )
                {
                    cursor.putLong( Double.doubleToLongBits( min[i] ) );
                    cursor.putLong( Double.doubleToLongBits( max[i] ) );
                }
            }
        } );
    }
}
