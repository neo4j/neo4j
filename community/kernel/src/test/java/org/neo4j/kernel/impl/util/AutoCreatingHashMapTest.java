/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.util;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.nested;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.values;

public class AutoCreatingHashMapTest
{
    @Test
    public void shouldCreateValuesIfMissing() throws Exception
    {
        // GIVEN
        Map<String, AtomicLong> map = new AutoCreatingHashMap<>( values( AtomicLong.class ) );
        String key = "should be created";

        // WHEN
        map.get( key ).incrementAndGet();

        // THEN
        assertEquals( 1, map.get( key ).get() );
        assertTrue( map.containsKey( key ) );
        assertFalse( map.containsKey( "any other key" ) );
    }

    @Test
    public void shouldCreateValuesEvenForNestedMaps() throws Exception
    {
        // GIVEN
        Map<String, Map<String, Map<String, AtomicLong>>> map = new AutoCreatingHashMap<>(
                nested( String.class, nested( String.class, values( AtomicLong.class ) ) ) );
        String keyLevelOne = "first";
        String keyLevelTwo = "second";
        String keyLevelThree = "third";

        // WHEN
        map.get( keyLevelOne ).get( keyLevelTwo ).get( keyLevelThree ).addAndGet( 10 );

        // THEN
        assertTrue( map.containsKey( keyLevelOne ) );
        assertFalse( map.containsKey( keyLevelTwo ) ); // or any other value for that matter
        Map<String, Map<String, AtomicLong>> levelOne = map.get( keyLevelOne );
        assertTrue( levelOne.containsKey( keyLevelTwo ) );
        assertFalse( levelOne.containsKey( keyLevelThree ) );  // or any other value for that matter
        Map<String, AtomicLong> levelTwo = levelOne.get( keyLevelTwo );
        assertTrue( levelTwo.containsKey( keyLevelThree ) );
        assertFalse( levelTwo.containsKey( keyLevelOne ) );  // or any other value for that matter
        AtomicLong levelThree = levelTwo.get( keyLevelThree );
        assertEquals( 10, levelThree.get() );
    }
}
