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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

import static org.junit.jupiter.api.Assertions.assertNull;

class GenericLayoutTest
{
    private static final IndexSpecificSpaceFillingCurveSettings spatialSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );

    @Test
    void shouldHaveUniqueIdentifierForDifferentNumberOfSlots()
    {
        Map<Long,Integer> layouts = new HashMap<>();
        for ( int i = 0; i < 100; i++ )
        {
            final GenericLayout genericLayout = new GenericLayout( i, spatialSettings );
            final Integer previous = layouts.put( genericLayout.identifier(), i );
            assertNull( previous,
                    String.format(
                            "Expected identifier to be unique for layout with different number of slots, but two had the same identifier, " +
                                    "firstSlotCount=%s, secondSlotCount=%s.", previous, i ) );
        }
    }
}
