/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class RecordFormatsGenerationTest
{
    @Test
    public void correctGenerations()
    {
        List<Integer> expectedGenerations = Arrays.asList(
                StandardV2_0.RECORD_FORMATS.generation(),
                StandardV2_1.RECORD_FORMATS.generation(),
                StandardV2_2.RECORD_FORMATS.generation(),
                StandardV2_3.RECORD_FORMATS.generation(),
                StandardV3_0.RECORD_FORMATS.generation(),
                HighLimit.RECORD_FORMATS.generation()
        );

        assertEquals( expectedGenerations, distinct( allGenerations() ) );
    }

    @Test
    public void uniqueGenerations()
    {
        assertEquals( allGenerations(), distinct( allGenerations() ) );
    }

    private static List<Integer> allGenerations()
    {
        return Arrays.stream( StoreVersion.values() )
                .map( StoreVersion::versionString )
                .map( RecordFormatSelector::selectForVersion )
                .map( RecordFormats::generation )
                .sorted()
                .collect( toList() );
    }

    private static List<Integer> distinct( List<Integer> integers )
    {
        return integers.stream()
                .distinct()
                .collect( toList() );
    }
}
