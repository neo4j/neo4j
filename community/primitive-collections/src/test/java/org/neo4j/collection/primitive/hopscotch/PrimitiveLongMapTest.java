/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.collection.primitive.hopscotch;

import org.neo4j.collection.primitive.hopscotch.PrimitiveLongIntHashMap;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongObjectHashMap;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongObjectMap;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

public class PrimitiveLongMapTest
{
    @Test
    public void shouldContainAddedValues() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = new PrimitiveLongObjectHashMap<>( 6 );
        map.put( 1994703545, 59 );
        map.put( 1583732120, 98 );
        map.put( 756530774, 56 );
        map.put( 1433091763, 22 );

        // WHEN/THEN
        boolean existedBefore = map.containsKey( 1433091763 );
        Integer valueBefore = map.get( 1433091763 );
        Integer previous = map.put( 1433091763, 35 );
        boolean existsAfter = map.containsKey( 1433091763 );
        Integer valueAfter = map.get( 1433091763 );
        assertTrue( "1433091763 should exist before putting here", existedBefore );
        assertEquals( (Integer)22, valueBefore );
        assertEquals( (Integer)22, previous );
        assertTrue( "(1433091763, 35) should exist", existsAfter );
        assertEquals( (Integer)35, valueAfter );
    }

    @Test
    public void shouldContainAddedValues_2() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = new PrimitiveLongObjectHashMap<>( 28 );
        map.put( 1950736976, 4 );
        map.put( 1054824202, 58 );
        map.put( 348690619, 54 );
        map.put( 1224909480, 79 );
        map.put( 1508493474, 82 );

        // WHEN/THEN
        boolean existedBefore = map.containsKey( 1508493474 );
        Integer valueBefore = map.get( 1508493474 );
        Integer previous = map.put( 1508493474, 62 );
        boolean existsAfter = map.containsKey( 1508493474 );
        Integer valueAfter = map.get( 1508493474 );
        assertTrue( "1508493474 should exist before putting here", existedBefore );
        assertEquals( "value before should be 82", (Integer)82, valueBefore );
        assertEquals( "value returned from put should be 82", (Integer)82, previous );
        assertTrue( "1508493474 should exist", existsAfter );
        assertEquals( "value after putting should be 62", (Integer)62, valueAfter );
    }

    @Test
    public void shouldContainAddedValues_3() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = new PrimitiveLongObjectHashMap<>( 6 );
        map.remove( 1338037218 );
        map.put( 680125236, 83 );
        map.put( 680125236, 76 );
        map.put( 680125236, 47 );
        map.put( 680125236, 30 );
        map.put( 2080483597, 52 );
        map.put( 867107519, 80 );
        map.remove( 710100384 );
        map.put( 671477921, 88 );
        map.put( 1163609643, 17 );
        map.put( 680125236, 57 );
        map.put( 1163609643, 70 );
        map.put( 2080483597, 89 );
        map.put( 1472451898, 62 );
        map.put( 1379499183, 93 );
        map.put( 680125236, 17 );
        map.put( 567842571, 43 );
        map.put( 2045599221, 60 );
        map.remove( 641295711 );
        map.remove( 867107519 );
        map.put( 2045599221, 30 );
        map.remove( 2094689486 );
        map.put( 1572965945, 79 );
        map.remove( 1329473388 );
        map.put( 1572965945, 39 );
        map.put( 264067586, 60 );
        map.put( 1751846500, 5 );
        map.put( 1163609643, 25 );
        map.put( 1379499183, 54 );
        map.remove( 671477921 );
        map.put( 1572965945, 59 );
        map.put( 880140639, 87 );

        // WHEN/THEN
        boolean existedBefore = map.containsKey( 468007595 );
        Integer valueBefore = map.get( 468007595 );
        Integer previous = map.put( 468007595, 67 );
        boolean existsAfter = map.containsKey( 468007595 );
        Integer valueAfter = map.get( 468007595 );
        assertFalse( "468007595 should not exist before putting here", existedBefore );
        assertNull( "value before putting should be null", valueBefore );
        assertNull( "value returned from putting should be null", previous );
        assertTrue( "468007595 should exist", existsAfter );
        assertEquals( "value after putting should be 67", (Integer)67, valueAfter );
    }

    @Test
    public void shouldHaveCorrectSize() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = new PrimitiveLongObjectHashMap<>( 24 );
        map.put( 152407843, 17 );
        map.put( 435803197, 29 );
        map.put( 2063473573, 75 );
        map.put( 162922679, 36 );
        map.put( 923042422, 47 );
        map.put( 204556993, 28 );
        map.put( 109670524, 80 );
        map.put( 214127443, 88 );
        map.put( 297958695, 97 );
        map.put( 873122371, 73 );
        map.put( 398704786, 25 );
        map.put( 376378917, 62 );
        map.put( 1948985185, 3 );
        map.put( 918339266, 4 );
        map.put( 1126937431, 48 );
        map.put( 568627750, 6 );
        map.put( 887668742, 1 );
        map.put( 888089153, 88 );
        map.put( 1671871078, 26 );
        map.put( 479217936, 11 );
        map.put( 1874408328, 56 );
        map.put( 1517450283, 83 );
        map.put( 1352952211, 57 );
        map.put( 686066722, 92 );
        map.put( 1593196310, 71 );
        map.put( 1535351391, 62 );
        map.put( 296560052, 59 );
        map.put( 1513542622, 49 );
        map.put( 1899330306, 57 );
        map.put( 746190595, 31 );
        map.put( 1216091366, 90 );
        map.put( 353922939, 16 );
        map.put( 680935464, 16 );
        map.put( 235368309, 8 );
        map.put( 1988133681, 32 );
        map.put( 330747855, 81 );
        map.put( 492627887, 74 );
        map.put( 1005495348, 8 );
        map.put( 2107419277, 82 );
        map.put( 1421265494, 15 );
        map.put( 1669915469, 92 );
        map.put( 2008247215, 9 );
        map.put( 2010142383, 77 );
        map.put( 829081830, 25 );
        map.put( 1349259272, 38 );
        map.put( 1987482877, 8 );
        map.put( 974334859, 83 );
        map.put( 1376908873, 10 );
        map.put( 2120105656, 22 );
        map.put( 1634193445, 8 );
        map.put( 1160987255, 34 );
        map.put( 2030156381, 16 );
        map.put( 2012943328, 22 );
        map.put( 75749275, 54 );
        map.put( 1415817090, 35 );
        map.put( 562352348, 43 );
        map.put( 658501173, 96 );
        map.put( 441278652, 24 );
        map.put( 633855945, 82 );
        map.put( 579807215, 31 );
        map.put( 1125922962, 33 );
        map.put( 1995076951, 91 );
        map.put( 322776761, 4 );
        map.put( 1011369342, 36 );

        // WHEN/THEN
        int sizeBefore = map.size();
        boolean existedBefore = map.containsKey( 679686325 );
        Integer valueBefore = map.get( 679686325 );
        Integer previous = map.put( 679686325, 63 );
        boolean existsAfter = map.containsKey( 679686325 );
        Integer valueAfter = map.get( 679686325 );
        int sizeAfter = map.size();
        assertEquals( "Size before put should have been 64", 64, sizeBefore );
        assertFalse( "679686325 should not exist before putting here", existedBefore );
        assertNull( "value before putting should be null", valueBefore );
        assertNull( "value returned from putting should be null", previous );
        assertTrue( "679686325 should exist", existsAfter );
        assertEquals( "value after putting should be 63", (Integer)63, valueAfter );
        assertEquals( "Size after put should have been 65", 65, sizeAfter );
    }

    @Test
    public void shouldMoveValuesWhenMovingEntriesAround() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = new PrimitiveLongObjectHashMap<>( 25 );
        map.put( 206243105, 47 );
        map.put( 2083304695, 63 );
        map.put( 689837337, 7 );
        map.remove( 206243105 );

        // WHEN/THEN
        int sizeBefore = map.size();
        boolean existedBefore = map.containsKey( 689837337 );
        Integer valueBefore = map.get( 689837337 );
        Integer previous = map.put( 689837337, 20 );
        boolean existsAfter = map.containsKey( 689837337 );
        Integer valueAfter = map.get( 689837337 );
        int sizeAfter = map.size();
        assertEquals( "Size before put should have been 2", 2, sizeBefore );
        assertTrue( "689837337 should exist before putting here", existedBefore );
        assertEquals( "value before should be 7", (Integer)7, valueBefore );
        assertEquals( "value returned from put should be 7", (Integer)7, previous );
        assertTrue( "689837337 should exist", existsAfter );
        assertEquals( "value after putting should be 20", (Integer)20, valueAfter );
        assertEquals( "Size after put should have been 2", 2, sizeAfter );
    }

    @Test
    public void shouldReturnCorrectPreviousValue() throws Exception
    {
        // GIVEN
        PrimitiveLongIntMap map = new PrimitiveLongIntHashMap( 27 );
        map.remove( 2050585513 );
        map.put( 429170228, 99 );
        map.put( 1356282827, 24 );
        map.remove( 1341095873 );

        // WHEN/THEN
        int sizeBefore = map.size();
        boolean existedBefore = map.containsKey( 429170228 );
        int valueBefore = map.get( 429170228 );
        int previous = map.put( 429170228, 1 );
        boolean existsAfter = map.containsKey( 429170228 );
        int valueAfter = map.get( 429170228 );
        int sizeAfter = map.size();
        assertEquals( "Size before put should have been 2", 2, sizeBefore );
        assertTrue( "429170228 should exist before putting here", existedBefore );
        assertEquals( "value before should be 99", 99, valueBefore );
        assertEquals( "value returned from put should be 99", 99, previous );
        assertTrue( "429170228 should exist", existsAfter );
        assertEquals( "value after putting should be 1", 1, valueAfter );
        assertEquals( "Size after put should have been 2", 2, sizeAfter );
    }

    @Test
    public void shouldOnlyContainAddedValues() throws Exception
    {
        // GIVEN
        PrimitiveLongIntMap map = new PrimitiveLongIntHashMap( 24 );
        map.put( 1179059774, 54 );
        map.put( 612612792, 91 );
        map.put( 853030395, 81 );
        map.put( 1821941016, 69 );
        map.put( 815540261, 54 );
        map.put( 2120470777, 63 );
        map.put( 866144206, 41 );
        map.put( 905659306, 86 );
        map.put( 602586792, 24 );
        map.put( 1033857549, 61 );
        map.put( 1570231638, 69 );
        map.put( 30675820, 53 );
        map.put( 433666923, 14 );
        map.put( 1668952952, 52 );
        map.put( 1733960171, 14 );
        map.put( 1240027317, 64 );
        map.put( 250830995, 71 );
        map.put( 1446519846, 17 );
        map.put( 1857052106, 78 );
        map.put( 37351838, 26 );
        map.put( 1523695604, 78 );
        map.put( 1024540180, 12 );
        map.put( 603632507, 81 );
        map.put( 483087335, 37 );
        map.put( 216300592, 55 );
        map.put( 1729046213, 72 );
        map.put( 1397559084, 78 );
        map.put( 802042428, 34 );
        map.put( 1127990805, 6 );
        map.put( 2081866795, 53 );
        map.put( 1528122026, 39 );
        map.put( 642547543, 78 );
        map.put( 1909701557, 35 );
        map.put( 2070740876, 40 );
        map.put( 316027755, 18 );
        map.put( 824089651, 63 );
        map.put( 1082682044, 85 );
        map.put( 154864377, 44 );
        map.put( 26918244, 73 );
        map.put( 808069768, 20 );
        map.put( 38089155, 17 );
        map.put( 1772700678, 35 );
        map.put( 1790535392, 82 );
        map.put( 159186757, 10 );
        map.put( 73305650, 52 );
        map.put( 2025019209, 38 );
        map.put( 922996536, 53 );
        map.put( 1852424925, 34 );
        map.put( 1181179273, 9 );
        map.put( 107520967, 11 );
        map.put( 1702904247, 55 );
        map.put( 1819417390, 50 );
        map.put( 1163114165, 57 );
        map.put( 2036796587, 40 );
        map.put( 2130510197, 26 );
        map.put( 1710533919, 70 );
        map.put( 497498438, 48 );
        map.put( 147722732, 8 );
        map.remove( 802042428 );
        map.put( 1355114893, 90 );
        map.put( 419675404, 62 );
        map.put( 1722846265, 41 );
        map.put( 1287254514, 61 );
        map.put( 1925017947, 8 );
        map.put( 1290391303, 59 );
        map.put( 1938779966, 27 );

        // WHEN/THEN
        int sizeBefore = map.size();
        boolean existedBefore = map.containsKey( 1452811669 );
        int valueBefore = map.get( 1452811669 );
        int previous = map.put( 1452811669, 16 );
        boolean existsAfter = map.containsKey( 1452811669 );
        int valueAfter = map.get( 1452811669 );
        int sizeAfter = map.size();
        assertEquals( "Size before put should have been 64", 64, sizeBefore );
        assertFalse( "1452811669 should not exist before putting here", existedBefore );
        assertEquals( "value before should be -1", -1, valueBefore );
        assertEquals( "value returned from put should be -1", -1, previous );
        assertTrue( "1452811669 should exist", existsAfter );
        assertEquals( "value after putting should be 16", 16, valueAfter );
        assertEquals( "Size after put should have been 65", 65, sizeAfter );
    }

    @Test
    public void shouldOnlyContainAddedValues_2() throws Exception
    {
        // GIVEN
        PrimitiveLongIntMap map = new PrimitiveLongIntHashMap( 27, DEFAULT_HASHING, new DebugMonitor(
                new int[] {63}, new long[] {947430652} ) );
        map.put( 913910231, 25 );
        map.put( 102310782, 40 );
        map.put( 634960377, 32 );
        map.put( 947168147, 96 );
        map.put( 947430652, 26 );
        map.put( 1391472521, 72 );
        map.put( 7905512, 10 );
        map.put( 7905512, 2 );
        map.put( 1391472521, 66 );
        map.put( 824376092, 79 );
        map.remove( 750639810 );
        map.put( 947168147, 61 );
        map.put( 831409018, 57 );
        map.put( 241941283, 76 );
        map.put( 824376092, 45 );
        map.remove( 2125994926 );
        map.put( 824376092, 47 );
        map.put( 1477982280, 1 );
        map.remove( 2129508263 );
        map.put( 1477982280, 41 );
        map.put( 642178985, 69 );
        map.put( 1447441709, 85 );
        map.put( 642178985, 27 );
        map.put( 875840384, 72 );
        map.put( 1967716733, 55 );
        map.put( 1965379174, 5 );
        map.put( 913910231, 40 );

        // WHEN/THEN
        boolean existedBefore = map.containsKey( 947430652 );
        int valueBefore = map.get( 947430652 );
        int removed = map.remove( 947430652 );
        boolean existsAfter = map.containsKey( 947430652 );
        int valueAfter = map.get( 947430652 );
        assertTrue( "947430652 should exist before removing here", existedBefore );
        assertEquals( "value before should be 26", 26, valueBefore );
        assertEquals( "value returned from remove should be 26", 26, removed );
        assertFalse( "947430652 should not exist", existsAfter );
        assertEquals( "value after removing should be -1", -1, valueAfter );
    }
}
