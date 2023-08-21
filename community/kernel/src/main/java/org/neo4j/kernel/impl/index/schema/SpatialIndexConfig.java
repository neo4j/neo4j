/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84_3D;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Utility class with static method for extracting relevant spatial index configurations from {@link CoordinateReferenceSystem} and
 * {@link SpaceFillingCurveSettings}. Configurations will be put into a map, with keys from {@link IndexSettingImpl}.
 * By using this class when extracting configurations we make sure that the same name and format is used for the same configuration.
 */
public final class SpatialIndexConfig {
    private SpatialIndexConfig() {}

    /**
     * Extract spatial index configuration and put into provided map.
     *
     * @param map {@link Map} into which extracted configurations should be inserted.
     * @param crs {@link CoordinateReferenceSystem} from which to extract configurations.
     * @param settings {@link SpaceFillingCurveSettings} from which to extract configurations.
     */
    static void addSpatialConfig(
            Map<String, Value> map, CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings) {
        double[] min = settings.indexExtents().getMin();
        double[] max = settings.indexExtents().getMax();
        addSpatialConfig(map, crs, min, max);
    }

    public static void addSpatialConfig(
            Map<String, Value> map, CoordinateReferenceSystem crs, double[] min, double[] max) {
        String minKey = IndexSettingUtil.spatialMinSettingForCrs(crs).getSettingName();
        String maxKey = IndexSettingUtil.spatialMaxSettingForCrs(crs).getSettingName();
        map.put(minKey, Values.doubleArray(min));
        map.put(maxKey, Values.doubleArray(max));
    }

    /**
     * Throws an {@link IllegalArgumentException} if the spatial settings in the given {@link IndexConfig} are invalid.
     */
    static void validateSpatialConfig(IndexConfig indexConfig) {
        extractSpatialConfig(indexConfig);
    }

    static IndexConfig addSpatialConfig(
            IndexConfig indexConfig, CoordinateReferenceSystem crs, SpaceFillingCurveSettings settings) {
        Map<String, Value> spatialConfig = new HashMap<>();
        addSpatialConfig(spatialConfig, crs, settings);
        for (var entry : spatialConfig.entrySet()) {
            indexConfig = indexConfig.withIfAbsent(entry.getKey(), entry.getValue());
        }
        return indexConfig;
    }

    /**
     * Translate {@link IndexConfig index config}, into {@link SpaceFillingCurveSettings settings} for each {@link CoordinateReferenceSystem crs}.
     *
     * @param indexConfig {@link IndexConfig} the index config to translate into space filling curve settings.
     * @return {@link Map} map containing space filling curve settings for every supported crs, derived from provided index config .
     * @throws NullPointerException if index config is missing configuration for any crs.
     */
    static Map<CoordinateReferenceSystem, SpaceFillingCurveSettings> extractSpatialConfig(IndexConfig indexConfig) {
        Map<CoordinateReferenceSystem, SpaceFillingCurveSettings> result = new HashMap<>();

        result.put(CARTESIAN, settingFromIndexConfig(indexConfig, CARTESIAN));
        result.put(CARTESIAN_3D, settingFromIndexConfig(indexConfig, CARTESIAN_3D));
        result.put(WGS_84, settingFromIndexConfig(indexConfig, WGS_84));
        result.put(WGS_84_3D, settingFromIndexConfig(indexConfig, WGS_84_3D));

        return result;
    }

    private static SpaceFillingCurveSettings settingFromIndexConfig(
            IndexConfig indexConfig, CoordinateReferenceSystem crs) {
        final double[] min = asDoubleArray(
                indexConfig.get(IndexSettingUtil.spatialMinSettingForCrs(crs).getSettingName()));
        final double[] max = asDoubleArray(
                indexConfig.get(IndexSettingUtil.spatialMaxSettingForCrs(crs).getSettingName()));
        final Envelope envelope = new Envelope(min, max);
        return new SpaceFillingCurveSettings(crs.getDimension(), envelope);
    }

    private static double[] asDoubleArray(Value value) {
        if (value instanceof DoubleArray) {
            return ((DoubleArray) value).asObjectCopy();
        }
        throw new IllegalStateException(
                String.format("Expected value to be of type %s but was %s.", DoubleArray.class, value));
    }
}
