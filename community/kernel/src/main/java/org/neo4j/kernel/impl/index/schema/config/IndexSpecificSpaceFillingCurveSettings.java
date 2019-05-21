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

import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Maps;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * Hold all spatial settings and use it to provide {@link SpaceFillingCurve} for this generic index.
 * The settings are determined at index creation time and does not change.
 */
public class IndexSpecificSpaceFillingCurveSettings
{
    /**
     * Map of settings that are specific to this index.
     */
    private final ImmutableMap<CoordinateReferenceSystem,SpaceFillingCurveSettings> specificIndexConfigCache;

    public IndexSpecificSpaceFillingCurveSettings( Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> specificIndexConfigCache )
    {
        this.specificIndexConfigCache = Maps.immutable.withAll( specificIndexConfigCache );
    }

    public static IndexSpecificSpaceFillingCurveSettings fromConfig( Config config )
    {
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> specificIndexConfigCache = new HashMap<>();
        ConfiguredSpaceFillingCurveSettingsCache configuredSettings = new ConfiguredSpaceFillingCurveSettingsCache( config );
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            specificIndexConfigCache.put( crs, configuredSettings.forCRS( crs ) );
        }

        return new IndexSpecificSpaceFillingCurveSettings( specificIndexConfigCache );
    }

    /**
     * Gets {@link SpaceFillingCurve} for a particular coordinate reference system's crsTableId and code point.
     *
     * @param crsTableId table id of the {@link CoordinateReferenceSystem}.
     * @param crsCodePoint code of the {@link CoordinateReferenceSystem}.
     * @return the {@link SpaceFillingCurve} for the given coordinate reference system.
     */
    public SpaceFillingCurve forCrs( int crsTableId, int crsCodePoint )
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( crsTableId, crsCodePoint );
        return forCrs( crs );
    }

    public SpaceFillingCurve forCrs( CoordinateReferenceSystem crs )
    {
        // Index-specific
        SpaceFillingCurveSettings specificSetting = specificIndexConfigCache.get( crs );
        if ( specificSetting != null )
        {
            return specificSetting.curve();
        }
        throw new IllegalStateException( "Index does not have any settings for coordinate reference system " + crs );
    }

    /**
     * To make it possible to extract index configuration from index at runtime.
     */
    public void visitIndexSpecificSettings( SettingVisitor visitor )
    {
        visitor.count( specificIndexConfigCache.size() );
        for ( CoordinateReferenceSystem crs : specificIndexConfigCache.keysView() )
        {
            visitor.visit( crs, specificIndexConfigCache.get( crs ) );
        }
    }

    public interface SettingVisitor
    {
        void count( int count );

        void visit( CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings );
    }
}
