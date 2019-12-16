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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class SpatialIndexConfigTest
{
    @Inject
    RandomRule random;

    @Test
    void mustAddSpatialConfigToMap()
    {
        HashMap<String,Value> map = new HashMap<>();
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            Config config = Config.defaults();
            SpaceFillingCurveSettings spaceFillingCurveSettings = new ConfiguredSpaceFillingCurveSettingsCache( config ).forCRS( crs );
            SpatialIndexConfig.addSpatialConfig( map, crs, spaceFillingCurveSettings );

            assertNotNull( map.remove( IndexSettingUtil.spatialMinSettingForCrs( crs ).getSettingName() ) );
            assertNotNull( map.remove( IndexSettingUtil.spatialMaxSettingForCrs( crs ).getSettingName() ) );
            assertTrue( map.isEmpty() );
        }
    }

    @Test
    void mustAddAndExtractSpatialConfigToIndexConfig()
    {
        IndexConfig indexConfig = IndexConfig.empty();
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> expectedMap = new HashMap<>();
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            Config config = Config.defaults();
            SpaceFillingCurveSettings spaceFillingCurveSettings = new ConfiguredSpaceFillingCurveSettingsCache( config ).forCRS( crs );
            expectedMap.put( crs, spaceFillingCurveSettings );
            indexConfig = SpatialIndexConfig.addSpatialConfig( indexConfig, crs, spaceFillingCurveSettings );
        }

        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> extractedMap = SpatialIndexConfig.extractSpatialConfig( indexConfig );
        assertEquals( expectedMap, extractedMap );
    }
}
