/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
