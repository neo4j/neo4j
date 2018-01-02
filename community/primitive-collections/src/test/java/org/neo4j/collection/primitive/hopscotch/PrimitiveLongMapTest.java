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
package org.neo4j.collection.primitive.hopscotch;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntLongMap;
import org.neo4j.collection.primitive.PrimitiveIntLongVisitor;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveIntVisitor;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongIntVisitor;
import org.neo4j.collection.primitive.PrimitiveLongLongMap;
import org.neo4j.collection.primitive.PrimitiveLongLongVisitor;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class PrimitiveLongMapTest
{
    @Test
    public void shouldContainAddedValues() throws Exception
    {
        // GIVEN
        Map<Long, Integer> expectedEntries = new HashMap<>();
        expectedEntries.put( 1994703545L, 59 );
        expectedEntries.put( 1583732120L, 98 );
        expectedEntries.put( 756530774L, 56 );
        expectedEntries.put( 1433091763L, 22 );

        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
        for ( Map.Entry<Long, Integer> entry : expectedEntries.entrySet() )
        {
            map.put( entry.getKey(), entry.getValue() );
        }

        // WHEN/THEN
        boolean existedBefore = map.containsKey( 1433091763 );
        Integer valueBefore = map.get( 1433091763 );
        Integer previous = map.put( 1433091763, 35 );
        boolean existsAfter = map.containsKey( 1433091763 );
        Integer valueAfter = map.get( 1433091763 );
        assertTrue( "1433091763 should exist before putting here", existedBefore );
        assertEquals( (Integer) 22, valueBefore );
        assertEquals( (Integer) 22, previous );
        assertTrue( "(1433091763, 35) should exist", existsAfter );
        assertEquals( (Integer) 35, valueAfter );
        expectedEntries.put( 1433091763L, 35 );

        final Map<Long, Integer> visitedEntries = new HashMap<>();
        map.visitEntries( new PrimitiveLongObjectVisitor<Integer, RuntimeException>()
        {
            @Override
            public boolean visited( long key, Integer value )
            {
                visitedEntries.put( key, value );
                return false;
            }
        } );
        assertEquals( expectedEntries, visitedEntries );
    }

    @Test
    public void shouldContainAddedValues_2() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
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
        assertEquals( "value before should be 82", (Integer) 82, valueBefore );
        assertEquals( "value returned from put should be 82", (Integer) 82, previous );
        assertTrue( "1508493474 should exist", existsAfter );
        assertEquals( "value after putting should be 62", (Integer) 62, valueAfter );
    }

    @Test
    public void shouldContainAddedValues_3() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
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
        assertEquals( "value after putting should be 67", (Integer) 67, valueAfter );
    }

    @Test
    public void shouldHaveCorrectSize() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
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
        assertEquals( "value after putting should be 63", (Integer) 63, valueAfter );
        assertEquals( "Size after put should have been 65", 65, sizeAfter );
    }

    @Test
    public void shouldMoveValuesWhenMovingEntriesAround() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
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
        assertEquals( "value before should be 7", (Integer) 7, valueBefore );
        assertEquals( "value returned from put should be 7", (Integer) 7, previous );
        assertTrue( "689837337 should exist", existsAfter );
        assertEquals( "value after putting should be 20", (Integer) 20, valueAfter );
        assertEquals( "Size after put should have been 2", 2, sizeAfter );
    }

    @Test
    public void shouldReturnCorrectPreviousValue() throws Exception
    {
        // GIVEN
        PrimitiveLongIntMap map = Primitive.longIntMap();
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
        PrimitiveLongIntMap map = Primitive.longIntMap();
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
        PrimitiveLongIntMap map = Primitive.longIntMap();
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

    @Test
    public void shouldOnlyContainAddedValues_3() throws Exception
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
        map.put( 2083704227957337692L, 50 );
        map.put( 1039748383662879297L, 12 );
        map.put( 6296247210943123044L, 45 );
        map.put( 8004677065031068097L, 5 );
        map.put( 1039748383662879297L, 70 );
        map.put( 5386804704064477958L, 97 );
        map.remove( 1506507783133586973L );
        map.put( 4287434858289406631L, 29 );
        map.put( 8004677065031068097L, 17 );
        map.put( 986286772325632801L, 14 );
        map.put( 7880139640446289959L, 68 );
        map.put( 8004677065031068097L, 23 );
        map.put( 5386804704064477958L, 72 );
        map.put( 5386804704064477958L, 71 );
        map.put( 2300381985575721987L, 0 );
        map.put( 6144230340727188436L, 31 );
        map.put( 425423457410117293L, 88 );
        map.put( 2083704227957337692L, 65 );
        map.put( 7805027477403310582L, 72 );
        map.put( 2254081933055750443L, 66 );
        map.put( 5386804704064477958L, 46 );
        map.put( 5787098127909281443L, 45 );
        map.put( 5508645210651400664L, 45 );
        map.put( 6092264867460428040L, 65 );
        map.put( 4551026293109220157L, 52 );
        map.put( 4669163071261559807L, 33 );
        map.put( 5790325306669462860L, 96 );
        map.put( 4337317298737908324L, 78 );
        map.put( 986286772325632801L, 71 );
        map.put( 4287434858289406631L, 47 );
        map.put( 1827085004206892313L, 30 );
        map.put( 6070945099342863711L, 88 );
        map.remove( 6300957726732252611L );
        map.put( 2300381985575721987L, 22 );
        map.put( 2083704227957337692L, 2 );
        map.put( 2885272279767063039L, 71 );
        map.put( 3627867780921264529L, 5 );
        map.remove( 5330274310754559602L );
        map.put( 8902857048431919030L, 23 );
        map.remove( 4287434858289406631L );
        map.put( 5459968256561120197L, 8 );
        map.put( 5790325306669462860L, 17 );
        map.put( 9003964541346458616L, 45 );
        map.put( 3832091967762842783L, 79 );
        map.put( 1332274446340546922L, 62 );
        map.put( 6610784890222945257L, 20 );
        map.put( 3627867780921264529L, 65 );
        map.put( 7988336790991560848L, 89 );
        map.put( 5386804704064477958L, 15 );
        map.put( 6296247210943123044L, 19 );
        map.put( 7776019112299874624L, 67 );
        map.put( 5827611175622537127L, 18 );
        map.remove( 8004677065031068097L );
        map.put( 2451971987846333787L, 48 );
        map.put( 3627867780921264529L, 16 );
        map.put( 2506727685914893570L, 61 );
        map.put( 6629089416451699826L, 89 );
        map.put( 875078333857781813L, 38 );
        map.put( 439984342972777679L, 51 );
        map.put( 9077428346047966819L, 19 );
        map.put( 7045299269724516542L, 73 );
        map.put( 8055487013098459354L, 24 );
        map.put( 6610784890222945257L, 65 );
        map.put( 986286772325632801L, 29 );
        map.put( 133928815519522465L, 81 );
        map.put( 5780114596098993316L, 15 );
        map.put( 3790785290324207363L, 91 );
        map.put( 2795040354588080479L, 48 );
        map.put( 4218658174275197144L, 59 );
        map.put( 6610784890222945257L, 70 );
        map.remove( 3722940212039795685L );
        map.put( 1817899559164238906L, 30 );
        map.put( 4551026293109220157L, 35 );
        map.put( 986286772325632801L, 57 );
        map.put( 3811462607668925015L, 57 );
        map.put( 2795040354588080479L, 85 );
        map.put( 8460476221939231932L, 86 );
        map.remove( 8957537157979159052L );
        map.put( 2032224502814063026L, 57 );
        map.remove( 8924941903092284834L );
        map.put( 5386804704064477958L, 2 );
        map.put( 6629089416451699826L, 18 );
        map.put( 425423457410117293L, 31 );
        map.put( 4337317298737908324L, 35 );
        map.remove( 5337770067730257989L );
        map.put( 6150561851033498431L, 49 );
        map.put( 5067121328094576685L, 46 );
        map.remove( 3742103310924563011L );
        map.put( 1327614778938791146L, 49 );
        map.put( 255729841510922319L, 16 );
        map.put( 8785988080128503533L, 69 );
        map.put( 4218658174275197144L, 20 );
        map.put( 1265271287408386915L, 43 );
        map.put( 255729841510922319L, 5 );
        map.put( 8651736753344997668L, 41 );
        map.put( 4363375305508283265L, 4 );
        map.put( 4185381066643227500L, 29 );
        map.put( 3790785290324207363L, 58 );
        map.put( 3058911485922749695L, 1 );
        map.put( 8629268898854377850L, 66 );
        map.put( 1762013345156514959L, 5 );
        map.remove( 4354754593499656793L );
        map.put( 1332274446340546922L, 16 );
        map.put( 4953501292937412915L, 87 );
        map.put( 2330841365833073849L, 83 );
        map.put( 8096564328797694553L, 44 );
        map.put( 8935185623148330821L, 7 );
        map.put( 6150561851033498431L, 48 );
        map.remove( 5827611175622537127L );
        map.put( 8048363335369773749L, 25 );
        map.put( 3627867780921264529L, 48 );
        map.put( 4806848030248674690L, 14 );
        map.put( 5430628648110105698L, 30 );
        map.remove( 7261476188677343032L );
        map.put( 1265271287408386915L, 61 );
        map.put( 9077428346047966819L, 32 );
        map.put( 1827085004206892313L, 95 );
        map.put( 6377023652046870199L, 8 );
        map.remove( 8096564328797694553L );
        map.put( 458594253548258561L, 37 );
        map.put( 4418108647578170347L, 60 );
        map.put( 4363375305508283265L, 50 );
        map.remove( 3220719966247388754L );
        map.put( 5067121328094576685L, 86 );
        map.put( 8030171618634928529L, 9 );
        map.remove( 5790325306669462860L );
        map.remove( 1693435088303118108L );
        map.put( 1817899559164238906L, 48 );
        map.put( 2823063986711596775L, 58 );
        map.put( 5065867711051034527L, 1 );
        map.put( 6144553725832876585L, 16 );
        map.put( 6066303112518690730L, 96 );
        map.put( 1627429134135319103L, 64 );
        map.put( 2083704227957337692L, 48 );
        map.put( 5074984076240598083L, 46 );
        map.put( 273737562207470342L, 60 );
        map.put( 5065867711051034527L, 7 );
        map.put( 1425720210238734727L, 23 );
        map.put( 8840483239403421070L, 42 );
        map.put( 622393419539870960L, 66 );
        map.put( 4649317581471627693L, 84 );
        map.put( 6344284253098418581L, 10 );
        map.put( 6066303112518690730L, 14 );
        map.put( 2032224502814063026L, 72 );
        map.put( 3860451022347437817L, 26 );
        map.put( 1931469116507191845L, 30 );
        map.put( 7264376865632246862L, 81 );
        map.put( 875078333857781813L, 41 );
        map.put( 6066303112518690730L, 65 );
        map.put( 357446231240164192L, 80 );
        map.put( 90138258774469874L, 73 );
        map.put( 2550828149718879762L, 72 );
        map.put( 357446231240164192L, 17 );
        map.put( 4233359298058523722L, 83 );
        map.put( 7879882017779927485L, 33 );
        map.put( 4554977248866184403L, 64 );
        map.put( 2032224502814063026L, 11 );
        map.put( 8460476221939231932L, 65 );
        map.put( 4404294840535520232L, 58 );
        map.put( 439984342972777679L, 83 );
        map.put( 143440583901416159L, 59 );
        map.put( 6980461179076170770L, 9 );
        map.put( 4253079906814783119L, 93 );
        map.put( 6377023652046870199L, 20 );
        map.put( 2885272279767063039L, 5 );
        map.put( 1115850061381524772L, 37 );
        map.put( 4288489609244987651L, 22 );
        map.put( 1869499448099043543L, 73 );
        map.put( 2233583342469238733L, 84 );
        map.put( 8785988080128503533L, 61 );
        map.put( 7396264003126204068L, 81 );
        map.put( 6553509363155186775L, 96 );
        map.put( 1265663249510580286L, 89 );
        map.put( 8824139147632000339L, 49 );
        map.put( 8629268898854377850L, 10 );
        map.put( 6463027127151126151L, 57 );
        map.put( 2577561266405706623L, 46 );
        map.put( 2942302849662258387L, 40 );
        map.put( 2233583342469238733L, 56 );
        map.put( 7971826071187872579L, 53 );
        map.put( 1425720210238734727L, 27 );
        map.remove( 7194434791627009043L );
        map.put( 1429250394105883546L, 82 );
        map.put( 8048363335369773749L, 19 );
        map.put( 425423457410117293L, 51 );
        map.remove( 3570674569632664356L );
        map.remove( 5925614419318569326L );
        map.put( 245367449754197583L, 27 );
        map.put( 8724491045048677021L, 55 );
        map.put( 1037934857236019066L, 66 );
        map.put( 8902857048431919030L, 61 );
        map.put( 4806848030248674690L, 17 );
        map.put( 8840483239403421070L, 95 );
        map.put( 2931578375554111170L, 54 );
        map.put( 5352224688502007093L, 36 );
        map.put( 6675404627060358866L, 64 );
        map.put( 5011448804620449550L, 48 );
        map.put( 9003964541346458616L, 44 );
        map.put( 8614830761978541860L, 70 );
        map.put( 3790785290324207363L, 95 );
        map.put( 3524676886726253569L, 54 );
        map.put( 6858076293577130289L, 60 );
        map.put( 6721253107702965701L, 41 );
        map.put( 655525227420977141L, 94 );
        map.put( 2344362186561469072L, 29 );
        map.put( 6144230340727188436L, 76 );
        map.put( 6751209943070153529L, 22 );
        map.put( 5528119873376392874L, 44 );
        map.put( 6675404627060358866L, 20 );
        map.put( 6167523814676644161L, 50 );
        map.put( 4288489609244987651L, 82 );
        map.remove( 3362704467864439992L );
        map.put( 8629268898854377850L, 50 );
        map.remove( 8824139147632000339L );
        map.remove( 8563575034946766108L );
        map.put( 4391871381220263726L, 20 );
        map.remove( 6143313773038364355L );
        map.remove( 3225044803974988142L );
        map.remove( 8048363335369773749L );
        map.remove( 439984342972777679L );
        map.put( 7776019112299874624L, 8 );
        map.put( 5414055783993307402L, 13 );
        map.put( 425423457410117293L, 91 );
        map.put( 8407567928758710341L, 30 );
        map.put( 6070945099342863711L, 14 );
        map.put( 5644323748441073606L, 91 );
        map.put( 5297141920581728538L, 61 );
        map.put( 7880139640446289959L, 1 );
        map.put( 2300381985575721987L, 92 );
        map.put( 8253246663621301435L, 26 );
        map.remove( 2074764355175726009L );
        map.remove( 3823843425563676964L );
        map.put( 8314906688468605292L, 91 );
        map.put( 6864119235983684905L, 56 );
        map.put( 6610784890222945257L, 85 );
        map.put( 3790785290324207363L, 7 );
        map.put( 9077428346047966819L, 20 );
        map.put( 5594781060356781714L, 76 );
        map.put( 4288489609244987651L, 24 );
        map.put( 5427718399315377322L, 93 );
        map.put( 6858076293577130289L, 41 );
        map.put( 4233359298058523722L, 43 );
        map.put( 3058911485922749695L, 88 );
        map.remove( 1327614778938791146L );
        map.put( 4665341449948530032L, 26 );
        map.remove( 2860868006143077426L );
        map.put( 6167523814676644161L, 70 );
        map.remove( 8314906688468605292L );
        map.put( 6396314739926743170L, 25 );
        map.put( 8924527320597926970L, 40 );
        map.put( 1817899559164238906L, 84 );
        map.remove( 4391871381220263726L );
        map.put( 8850817829384121639L, 50 );
        map.put( 6513548978704592547L, 52 );
        map.remove( 6066303112518690730L );
        map.remove( 3946964103425920940L );
        map.put( 7971826071187872579L, 71 );
        map.put( 90138258774469874L, 78 );
        map.put( 8309039683334256753L, 44 );
        map.put( 327300646665050265L, 52 );
        map.put( 4239841777571533415L, 22 );
        map.put( 7391753878925882699L, 46 );
        map.put( 5987501380005333533L, 31 );
        map.put( 6734545541042861356L, 45 );
        map.remove( 6566682167801344029L );
        map.put( 4218658174275197144L, 16 );
        map.put( 4363586488886891680L, 88 );
        map.put( 8030171618634928529L, 19 );
        map.put( 6513548978704592547L, 95 );
        map.put( 6721253107702965701L, 55 );
        map.put( 2153470608693815785L, 9 );
        map.put( 5807454155419905847L, 7 );
        map.remove( 4528425347504500078L );
        map.put( 339083533777732657L, 72 );
        map.put( 5162811261582626928L, 68 );
        map.put( 5459968256561120197L, 89 );
        map.put( 946125626260258615L, 97 );
        map.put( 986286772325632801L, 26 );
        map.put( 8309039683334256753L, 74 );
        map.put( 1609193622622537433L, 84 );
        map.put( 2506727685914893570L, 9 );
        map.put( 143440583901416159L, 33 );
        map.put( 7716482408003289208L, 30 );
        map.put( 7880139640446289959L, 74 );
        map.put( 5472992709007694577L, 27 );
        map.put( 3367972495572249232L, 8 );
        map.put( 6002824320296423294L, 71 );
        map.put( 5162811261582626928L, 10 );
        map.remove( 8309039683334256753L );
        map.put( 3103455156394998975L, 1 );
        map.put( 4943074037151902792L, 38 );
        map.put( 1455801901314190156L, 98 );
        map.put( 3502583509759951230L, 22 );
        map.remove( 8464127935014315372L );
        map.put( 6858076293577130289L, 35 );
        map.put( 8487179770790306175L, 5 );
        map.put( 946125626260258615L, 85 );
        map.put( 722144778357869055L, 1 );
        map.remove( 6832604792388788147L );
        map.remove( 7879882017779927485L );
        map.put( 4636443662717865247L, 98 );
        map.put( 6950926592851406543L, 12 );
        map.put( 8536120340569832116L, 73 );
        map.put( 86730768989854734L, 66 );
        map.put( 4558683789229895837L, 26 );
        map.put( 4806848030248674690L, 11 );
        map.put( 425423457410117293L, 38 );
        map.put( 8713875164075871710L, 97 );
        map.put( 3790785290324207363L, 77 );
        map.put( 4632006356221328093L, 21 );
        map.put( 7628512490650429100L, 28 );
        map.remove( 4651124484202085669L );
        map.put( 4320012891688937760L, 22 );
        map.put( 6092264867460428040L, 86 );
        map.put( 6610784890222945257L, 71 );
        map.remove( 3515175120945606156L );
        map.put( 5787098127909281443L, 10 );
        map.put( 5057609667342409825L, 50 );
        map.put( 5903362554916539560L, 75 );
        map.remove( 5339209082212961633L );
        map.put( 3502583509759951230L, 36 );
        map.put( 4198420341072443663L, 75 );
        map.put( 5037754181090593008L, 34 );
        map.put( 39606137866137388L, 19 );
        map.remove( 622393419539870960L );
        map.put( 2783004740411041924L, 79 );
        map.put( 6232331175163415825L, 72 );
        map.put( 4367206208262757151L, 33 );
        map.remove( 5879159150292946046L );
        map.put( 722144778357869055L, 80 );
        map.put( 9006426844471489361L, 92 );
        map.put( 550025535839604778L, 32 );
        map.remove( 5855895659233120621L );
        map.put( 1455801901314190156L, 24 );
        map.put( 3860451022347437817L, 81 );
        map.put( 2672104991948169160L, 57 );
        map.remove( 3860451022347437817L );
        map.remove( 655525227420977141L );
        map.put( 2413633498546493443L, 68 );
        map.put( 4185381066643227500L, 54 );
        map.put( 1280345971255663584L, 39 );
        map.put( 5796123963544961504L, 76 );
        map.put( 1892786158672061630L, 55 );
        map.remove( 5352224688502007093L );
        map.put( 3711105805930144213L, 47 );
        map.put( 4608237982157900285L, 41 );
        map.put( 4175794211341763944L, 31 );
        map.put( 2315250912582233395L, 81 );
        map.put( 357446231240164192L, 87 );
        map.put( 4110861648946406824L, 75 );
        map.put( 6912381889380280106L, 22 );
        map.put( 6721253107702965701L, 43 );
        map.put( 8536120340569832116L, 87 );
        map.put( 9134483648483594929L, 77 );
        map.put( 9132976039160654816L, 69 );
        map.remove( 7698175804504341415L );
        map.remove( 9134483648483594929L );
        map.put( 215721718639621876L, 11 );
        map.put( 8367455298026304238L, 78 );
        map.put( 215721718639621876L, 13 );
        map.put( 1398628381776162625L, 12 );
        map.put( 3818698536247649025L, 91 );
        map.put( 146020861698406718L, 41 );
        map.put( 39606137866137388L, 93 );
        map.put( 2032224502814063026L, 29 );
        map.remove( 6363504799104250810L );
        map.put( 7198198302699040275L, 75 );
        map.put( 1659665859871881503L, 35 );
        map.put( 2032224502814063026L, 25 );
        map.put( 7006780191094382053L, 2 );
        map.put( 2626850727701928459L, 97 );
        map.put( 5371963064889126677L, 49 );
        map.put( 2777831232791546183L, 35 );
        map.remove( 1265271287408386915L );
        map.remove( 1078791602714388223L );
        map.put( 7355915493826998767L, 39 );
        map.remove( 1557741259882614531L );
        map.put( 318456745029053198L, 18 );
        map.put( 5731549637584761783L, 77 );
        map.put( 875078333857781813L, 80 );
        map.remove( 4288489609244987651L );
        map.put( 6296247210943123044L, 67 );
        map.put( 6513548978704592547L, 60 );
        map.put( 7484688824700837146L, 79 );
        map.put( 4551026293109220157L, 77 );
        map.put( 2961669147182343860L, 80 );
        map.put( 4481942776688563562L, 28 );
        map.put( 5879809531485088687L, 63 );
        map.put( 5799223884087101214L, 94 );
        map.put( 8394473765965282856L, 59 );
        map.remove( 7273585073251585620L );
        map.remove( 5518575735665118270L );
        map.put( 1946691597339845823L, 64 );
        map.put( 1191724556568067952L, 33 );
        map.remove( 1803989601564179749L );
        map.put( 7909563548070411816L, 98 );
        // WHEN/THEN
        int sizeBefore = map.size();
        boolean existedBefore = map.containsKey( 5826258075197365143L );
        Integer valueBefore = map.get( 5826258075197365143L );
        Integer previous = map.put( 5826258075197365143L, 6 );
        boolean existsAfter = map.containsKey( 5826258075197365143L );
        Integer valueAfter = map.get( 5826258075197365143L );
        int sizeAfter = map.size();
        assertEquals( "Size before put should have been 199", 199, sizeBefore );
        assertFalse( "5826258075197365143 should not exist before putting here", existedBefore );
        assertNull( "value before putting should be null", valueBefore );
        assertNull( "value returned from putting should be null", previous );
        assertTrue( "5826258075197365143 should exist", existsAfter );
        assertEquals( "value after putting should be 6", (Integer) 6, valueAfter );
        assertEquals( "Size after put should have been 200", 200, sizeAfter );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void longIntEntryVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveLongIntMap map = Primitive.longIntMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveLongIntVisitor<RuntimeException> visitor = mock( PrimitiveLongIntVisitor.class );

        // WHEN
        map.visitEntries( visitor );

        // THEN
        verify( visitor ).visited( 1, 100 );
        verify( visitor ).visited( 2, 200 );
        verify( visitor ).visited( 3, 300 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void longIntEntryVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveLongIntMap map = Primitive.longIntMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitEntries( new PrimitiveLongIntVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long key, int value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void intLongEntryVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveIntLongMap map = Primitive.intLongMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveIntLongVisitor<RuntimeException> visitor = mock( PrimitiveIntLongVisitor.class );

        // WHEN
        map.visitEntries( visitor );

        // THEN
        verify( visitor ).visited( 1, 100 );
        verify( visitor ).visited( 2, 200 );
        verify( visitor ).visited( 3, 300 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void intLongEntryVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveIntLongMap map = Primitive.intLongMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitEntries( new PrimitiveIntLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( int key, long value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void longLongEntryVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveLongLongMap map = Primitive.offHeapLongLongMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveLongLongVisitor<RuntimeException> visitor = mock( PrimitiveLongLongVisitor.class );

        // WHEN
        map.visitEntries( visitor );

        // THEN
        verify( visitor ).visited( 1, 100 );
        verify( visitor ).visited( 2, 200 );
        verify( visitor ).visited( 3, 300 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void longLongEntryVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveLongLongMap map = Primitive.offHeapLongLongMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitEntries( new PrimitiveLongLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long key, long value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void longObjectEntryVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveLongObjectVisitor<Integer, RuntimeException> visitor = mock( PrimitiveLongObjectVisitor.class );

        // WHEN
        map.visitEntries( visitor );

        // THEN
        verify( visitor ).visited( 1, 100 );
        verify( visitor ).visited( 2, 200 );
        verify( visitor ).visited( 3, 300 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void longObjectEntryVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitEntries( new PrimitiveLongObjectVisitor<Integer, RuntimeException>()
        {
            @Override
            public boolean visited( long key, Integer value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void intObjectEntryVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveIntObjectMap<Integer> map = Primitive.intObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveIntObjectVisitor<Integer, RuntimeException> visitor = mock( PrimitiveIntObjectVisitor.class );

        // WHEN
        map.visitEntries( visitor );

        // THEN
        verify( visitor ).visited( 1, 100 );
        verify( visitor ).visited( 2, 200 );
        verify( visitor ).visited( 3, 300 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void intObjectEntryVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveIntObjectMap<Integer> map = Primitive.intObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitEntries( new PrimitiveIntObjectVisitor<Integer, RuntimeException>()
        {
            @Override
            public boolean visited( int key, Integer value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void longIntKeyVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveLongIntMap map = Primitive.longIntMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveLongVisitor<RuntimeException> visitor = mock( PrimitiveLongVisitor.class );

        // WHEN
        map.visitKeys( visitor );

        // THEN
        verify( visitor ).visited( 1 );
        verify( visitor ).visited( 2 );
        verify( visitor ).visited( 3 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void longIntKeyVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveLongIntMap map = Primitive.longIntMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitKeys( new PrimitiveLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void longLongKeyVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveLongLongMap map = Primitive.offHeapLongLongMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveLongVisitor<RuntimeException> visitor = mock( PrimitiveLongVisitor.class );

        // WHEN
        map.visitKeys( visitor );

        // THEN
        verify( visitor ).visited( 1 );
        verify( visitor ).visited( 2 );
        verify( visitor ).visited( 3 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void longLongKeyVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveLongLongMap map = Primitive.offHeapLongLongMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitKeys( new PrimitiveLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void longObjectKeyVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveLongVisitor<RuntimeException> visitor = mock( PrimitiveLongVisitor.class );

        // WHEN
        map.visitKeys( visitor );

        // THEN
        verify( visitor ).visited( 1 );
        verify( visitor ).visited( 2 );
        verify( visitor ).visited( 3 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void longObjectKeyVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveLongObjectMap<Integer> map = Primitive.longObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitKeys( new PrimitiveLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void intObjectKeyVisitorShouldSeeAllEntriesIfItDoesNotBreakOut()
    {
        // GIVEN
        PrimitiveIntObjectMap<Integer> map = Primitive.intObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        PrimitiveIntVisitor<RuntimeException> visitor = mock( PrimitiveIntVisitor.class );

        // WHEN
        map.visitKeys( visitor );

        // THEN
        verify( visitor ).visited( 1 );
        verify( visitor ).visited( 2 );
        verify( visitor ).visited( 3 );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void intObjectKeyVisitorShouldNotSeeEntriesAfterRequestingBreakOut()
    {
        // GIVEN
        PrimitiveIntObjectMap<Integer> map = Primitive.intObjectMap();
        map.put( 1, 100 );
        map.put( 2, 200 );
        map.put( 3, 300 );
        map.put( 4, 400 );
        final AtomicInteger counter = new AtomicInteger();

        // WHEN
        map.visitKeys( new PrimitiveIntVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( int value )
            {
                return counter.incrementAndGet() > 2;
            }
        } );

        // THEN
        assertThat( counter.get(), is( 3 ) );
    }

    @Test
    public void recursivePutGrowInterleavingShouldNotDropOriginalValues()
    {
        // List of values which causes calls to put() call grow(), which will call put() which calls grow() again
        List<Long> lst = Arrays.asList(
                44988L, 44868L, 44271L, 44399L, 44502L, 44655L, 44348L, 44843L,
                44254L, 44774L, 44476L, 44664L, 44485L, 44237L, 44953L, 44468L,
                44970L, 44808L, 44527L, 44987L, 44672L, 44647L, 44467L, 44825L,
                44740L, 44220L, 44851L, 44902L, 44791L, 44416L, 44365L, 44382L,
                44885L, 44510L, 44553L, 44894L, 44288L, 44306L, 44450L, 44689L,
                44305L, 44374L, 44323L, 44493L, 44706L, 44681L, 44578L, 44723L,
                44331L, 44936L, 44289L, 44919L, 44433L, 44826L, 44757L, 44561L,
                44595L, 44612L, 44996L, 44646L, 44834L, 44314L, 44544L, 44629L,
                44357L // <-- this value will cause a grow, which during new table population will cause another grow.
        );

        verifyMapRetainsAllEntries( lst );
    }

    @Test
    public void recursivePutGrowInterleavingShouldNotDropOriginalValuesEvenWhenFirstGrowAddsMoreValuesAfterSecondGrow()
            throws Exception
    {
        // List of values that cause recursive growth like above, but this time the first grow wants to add more values
        // to the table *after* the second grow has occurred.
        List<Long> lst = Arrays.asList(
                85380L, 85124L, 85252L, 85259L, 85005L, 85260L, 85132L, 85141L,
                85397L, 85013L, 85269L, 85277L, 85149L, 85404L, 85022L, 85150L,
                85029L, 85414L, 85158L, 85286L, 85421L, 85039L, 85167L, 85294L,
                85166L, 85431L, 85303L, 85046L, 85311L, 85439L, 85438L, 85184L,
                85056L, 85063L, 85320L, 85448L, 85201L, 85073L, 85329L, 85456L,
                85328L, 85337L, 85081L, 85465L, 85080L, 85208L, 85473L, 85218L,
                85346L, 85090L, 85097L, 85225L, 85354L, 85098L, 85482L, 85235L,
                85363L, 85107L, 85490L, 85115L, 85499L, 85242L, 85175L, 85371L,
                85192L // <-- this value will cause a grow, which during new table population will cause another grow.
        );

        verifyMapRetainsAllEntries( lst );
    }

    private void verifyMapRetainsAllEntries( List<Long> lst )
    {
        PrimitiveLongIntMap map = Primitive.longIntMap();
        Set<Long> set = new HashSet<>();
        for ( Long value : lst )
        {
            assertThat( map.put( value, 1 ), is( -1 ) );
            assertTrue( set.add( value ) );
        }

        assertThat( map.size(), is( set.size() ) );
        for ( Long aLong : set )
        {
            assertThat( map.get( aLong ), is( 1 ) );
        }
    }
}
