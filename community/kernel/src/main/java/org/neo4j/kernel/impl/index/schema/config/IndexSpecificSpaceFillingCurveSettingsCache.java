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
package org.neo4j.kernel.impl.index.schema.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * A combination of {@link ConfiguredSpaceFillingCurveSettingsCache}, which contains all settings from {@link Config},
 * but also settings for a specific index. Settings for a specific index can change over time as new {@link CoordinateReferenceSystem}
 * are used in this index.
 */
public class IndexSpecificSpaceFillingCurveSettingsCache
{
    private final ConfiguredSpaceFillingCurveSettingsCache globalConfigCache;
    /**
     * Map of settings that are specific to this index, i.e. where there is or have been at least one value of in the index.
     * Modifications of this map happen by a single thread at a time, due to index updates being applied using work-sync,
     * but there can be concurrent readers at any point.
     */
    private final ConcurrentMap<CoordinateReferenceSystem,SpaceFillingCurveSettings> specificIndexConfigCache = new ConcurrentHashMap<>();

    public IndexSpecificSpaceFillingCurveSettingsCache(
            ConfiguredSpaceFillingCurveSettingsCache globalConfigCache,
            Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> specificIndexConfigCache )
    {
        this.globalConfigCache = globalConfigCache;
        this.specificIndexConfigCache.putAll( specificIndexConfigCache );
    }

    /**
     * Gets {@link SpaceFillingCurve} for a particular coordinate reference system's crsTableId and code point.
     *
     * @param crsTableId table id of the {@link CoordinateReferenceSystem}.
     * @param crsCodePoint code of the {@link CoordinateReferenceSystem}.
     * @param assignToIndexIfNotYetAssigned whether or not to make a snapshot of this index-specific setting if this is the
     * first time it's accessed for this index. It will then show up in {@link #visitIndexSpecificSettings(SettingVisitor)}.
     * @return the {@link SpaceFillingCurve} for the given coordinate reference system.
     */
    public SpaceFillingCurve forCrs( int crsTableId, int crsCodePoint, boolean assignToIndexIfNotYetAssigned )
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( crsTableId, crsCodePoint );
        return forCrs( crs, assignToIndexIfNotYetAssigned );
    }

    public SpaceFillingCurve forCrs( CoordinateReferenceSystem crs, boolean assignToIndexIfNotYetAssigned )
    {
        // Index-specific
        SpaceFillingCurveSettings specificSetting = specificIndexConfigCache.get( crs );
        if ( specificSetting != null )
        {
            return specificSetting.curve();
        }

        // Global config
        SpaceFillingCurveSettings configuredSetting = globalConfigCache.forCRS( crs );
        if ( assignToIndexIfNotYetAssigned )
        {
            specificIndexConfigCache.put( crs, configuredSetting );
        }
        return configuredSetting.curve();
    }

    /**
     * Mostly for checkpoints to serialize index-specific settings into the index header.
     */
    public void visitIndexSpecificSettings( SettingVisitor visitor )
    {
        visitor.count( specificIndexConfigCache.size() );
        specificIndexConfigCache.forEach( visitor::visit );
    }

    public interface SettingVisitor
    {
        void count( int count );

        void visit( CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings );
    }
}
