/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class RecordFormatsGenerationTest
{
    @Test
    public void correctGenerations()
    {
        List<RecordFormats> recordFormats = Arrays.asList(
                StandardV2_0.RECORD_FORMATS,
                StandardV2_1.RECORD_FORMATS,
                StandardV2_2.RECORD_FORMATS,
                StandardV2_3.RECORD_FORMATS,
                StandardV3_0.RECORD_FORMATS,
                HighLimitV3_0_0.RECORD_FORMATS,
                HighLimit.RECORD_FORMATS
        );

        Map<FormatFamily,List<Integer>> generationsForFamilies = recordFormats
                                                            .stream()
                                                            .collect( groupingBy( RecordFormats::getFormatFamily,
                                                                mapping( RecordFormats::generation, toList() ) ) );
        assertEquals( 2, generationsForFamilies.size() );
        for ( Map.Entry<FormatFamily,List<Integer>> familyListGeneration : generationsForFamilies.entrySet() )
        {
            assertEquals(  "Generation inside format family should be unique.",
                    familyListGeneration.getValue(), distinct( familyListGeneration.getValue() ) );
        }
    }

    @Test
    public void uniqueGenerations()
    {
        Map<FormatFamily,List<Integer>> familyGenerations = allFamilyGenerations();
        for ( Map.Entry<FormatFamily,List<Integer>> familyEntry : familyGenerations.entrySet() )
        {
            assertEquals( "Generation inside format family should be unique.",
                    familyEntry.getValue(), distinct( familyEntry.getValue() ) );
        }
    }

    private static Map<FormatFamily, List<Integer>> allFamilyGenerations()
    {
        return Arrays.stream( StoreVersion.values() )
                .map( StoreVersion::versionString )
                .map( RecordFormatSelector::selectForVersion )
                .collect( groupingBy( RecordFormats::getFormatFamily,  mapping( RecordFormats::generation, toList() ) ) );
    }

    private static List<Integer> distinct( List<Integer> integers )
    {
        return integers.stream()
                .distinct()
                .collect( toList() );
    }
}
