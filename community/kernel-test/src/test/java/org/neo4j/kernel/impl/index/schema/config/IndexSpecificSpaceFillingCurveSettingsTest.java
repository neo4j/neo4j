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
package org.neo4j.kernel.impl.index.schema.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

class IndexSpecificSpaceFillingCurveSettingsTest {
    private static final Config config = Config.defaults();
    private final ConfiguredSpaceFillingCurveSettingsCache globalSettings =
            new ConfiguredSpaceFillingCurveSettingsCache(config);

    @Test
    void shouldHaveInitialIndexSpecificSetting() {
        // given
        Map<CoordinateReferenceSystem, SpaceFillingCurveSettings> initialSettings = new HashMap<>();
        initialSettings.put(WGS_84, globalSettings.forCRS(WGS_84));
        initialSettings.put(CARTESIAN, globalSettings.forCRS(CARTESIAN));
        IndexSpecificSpaceFillingCurveSettings indexSettings =
                new IndexSpecificSpaceFillingCurveSettings(initialSettings);

        // when
        ToMapSettingVisitor visitor = new ToMapSettingVisitor();
        indexSettings.visitIndexSpecificSettings(visitor);

        // then
        assertEquals(initialSettings, visitor.map);
    }

    @Test
    void shouldThrowIfAskedForNonExistingIndexSetting() {
        // given
        Map<CoordinateReferenceSystem, SpaceFillingCurveSettings> initialSettings = new HashMap<>();
        initialSettings.put(WGS_84, globalSettings.forCRS(WGS_84));
        initialSettings.put(CARTESIAN, globalSettings.forCRS(CARTESIAN));
        IndexSpecificSpaceFillingCurveSettings indexSettings =
                new IndexSpecificSpaceFillingCurveSettings(initialSettings);

        // when
        IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> indexSettings.forCrs(CARTESIAN_3D));
        Assertions.assertTrue(exception
                .getMessage()
                .contains("Index does not have any settings for coordinate reference system cartesian-3d"));
    }
}
