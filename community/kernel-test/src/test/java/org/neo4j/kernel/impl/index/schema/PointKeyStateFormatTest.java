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

import java.nio.file.OpenOption;
import java.util.List;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class PointKeyStateFormatTest extends IndexKeyStateFormatTest<PointKey> {
    @Override
    void populateValues(List<Value> values) {
        // Some points selected randomly. One for each crs.
        values.add(Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 764544.2109309451, 181214.06418126775));
        values.add(Values.pointValue(
                CoordinateReferenceSystem.CARTESIAN_3D, -19020.757072408567, 292885.859461074, -599001.3101378167));
        values.add(Values.pointValue(CoordinateReferenceSystem.WGS_84, -117.4416383571589, 66.28879082642959));
        values.add(Values.pointValue(
                CoordinateReferenceSystem.WGS_84_3D, -62.00488758672253, 19.002727191347063, 600447.8589570583));
    }

    @Override
    Layout<PointKey, ?> getLayout() {
        // Use default settings.
        IndexSpecificSpaceFillingCurveSettings settings =
                IndexSpecificSpaceFillingCurveSettings.fromConfig(Config.defaults());
        return new PointLayout(settings);
    }

    @Override
    protected String zipName() {
        return "current-point-key-state-format.zip";
    }

    @Override
    protected String storeFileName() {
        return "point-key-state-store";
    }

    @Override
    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.of(PageCacheOpenOptions.BIG_ENDIAN);
    }

    @Override
    String toDetailedString(PointKey key) {
        return key.toString();
    }
}
