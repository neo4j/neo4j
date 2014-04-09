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

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.hopscotch.PrimitiveLongHashSet;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;
import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.NO_MONITOR;

public class PrimitiveLongSetTest
{
    @Test
    public void shouldResolveCollisions() throws Exception
    {
        // GIVEN
        /* These are the index placements of the first 100 values (0-99)
         * 0:  [0, 17, 34, 51, 68, 85]
         * 1:  [1, 16, 35, 50, 69, 84]
         * 2:  [2, 19, 32, 49, 70, 87]
         * 3:  [3, 18, 33, 48, 71, 86]
         * 4:  [4, 21, 38, 55, 64, 81, 98]
         * 5:  [5, 20, 39, 54, 65, 80, 99]
         * 6:  [6, 23, 36, 53, 66, 83, 96]
         * 7:  [7, 22, 37, 52, 67, 82, 97]
         * 8:  [8, 25, 42, 59, 76, 93]
         * 9:  [9, 24, 43, 58, 77, 92]
         * 10: [10, 27, 40, 57, 78, 95]
         * 11: [11, 26, 41, 56, 79, 94]
         * 12: [12, 29, 46, 63, 72, 89]
         * 13: [13, 28, 47, 62, 73, 88]
         * 14: [14, 31, 44, 61, 74, 91]
         * 15: [15, 30, 45, 60, 75, 90]
         */
        PrimitiveLongSet set = new PrimitiveLongHashSet( 4, DEFAULT_HASHING, onlyAllowGrowing( 1 ) );
        set.add( 1 );  // index 1
        set.add( 2 );  // index 2
        set.add( 19 ); // index 3 (originally 2)
        set.add( 4 );  // index 4
        set.add( 21 ); // index 5 (originally 4)
        set.add( 6 );  // index 6
        set.add( 23 ); // index 7 (originally 6)
        set.add( 8 );  // index 8
        set.add( 25 ); // index 9 (originally 8)
        set.add( 10 ); // index 10

        // WHEN
        assertTrue( "Couldn't add 35", set.add( 35 ) ); // 35 hashes to index 1

        // THEN
        assertTrue( "Didn't contain 35", set.contains( 35 ) );
    }

    @Test
    public void shouldContainAddedValues_generated_1() throws Exception
    {
        // GIVEN
        PrimitiveLongSet set = new PrimitiveLongHashSet( 15, DEFAULT_HASHING, NO_MONITOR );
        set.add( 1207043189 );
        set.add( 380713862 );
        set.add( 1902858197 );
        set.add( 1996873101 );
        set.add( 1357024628 );
        set.add( 1044248801 );
        set.add( 1558157493 );
        set.add( 2040311008 );
        set.add( 2017660098 );
        set.add( 1332670047 );
        set.add( 663662790 );
        set.add( 2063747422 );
        set.add( 1554358949 );
        set.add( 1761477445 );
        set.add( 1141526838 );
        set.add( 1698679618 );
        set.add( 1279767067 );
        set.add( 508574 );
        set.add( 2071755904 );

        // WHEN/THEN
        boolean existedBefore = set.contains( 679990875 );
        boolean added = set.add( 679990875 );
        boolean existsAfter = set.contains( 679990875 );
        assertFalse( "679990875 should not exist before adding here", existedBefore );
        assertTrue( "679990875 should be reported as added here", added );
        assertTrue( "679990875 should exist", existsAfter );
    }

    @Test
    public void shouldContainAddedValues_generated_6() throws Exception
    {
        // GIVEN
        PrimitiveLongSet set = new PrimitiveLongHashSet( 11, DEFAULT_HASHING, NO_MONITOR );
        set.add( 492321488 );
        set.add( 877087251 );
        set.add( 1809668113 );
        set.add( 1766034186 );
        set.add( 1879253467 );
        set.add( 669295491 );
        set.add( 176011165 );
        set.add( 1638959981 );
        set.add( 1093132636 );
        set.add( 6133241 );
        set.add( 486112773 );
        set.add( 205218385 );
        set.add( 1756491867 );
        set.add( 90390732 );
        set.add( 937266036 );
        set.add( 1269020584 );
        set.add( 521469166 );
        set.add( 1314928747 );

        // WHEN/THEN
        boolean existedBefore = set.contains( 2095121629 );
        boolean added = set.add( 2095121629 );
        boolean existsAfter = set.contains( 2095121629 );
        assertFalse( "2095121629 should not exist before adding here", existedBefore );
        assertTrue( "2095121629 should be reported as added here", added );
        assertTrue( "2095121629 should exist", existsAfter );
    }

    @Test
    public void shouldContainAddedValues_generated_4() throws Exception
    {
        // GIVEN
        PrimitiveLongSet set = new PrimitiveLongHashSet( 9, DEFAULT_HASHING, NO_MONITOR );
        set.add( 1934106304 );
        set.add( 783754072 );
        set.remove( 1934106304 );

        // WHEN/THEN
        boolean existedBefore = set.contains( 783754072 );
        boolean added = set.add( 783754072 );
        boolean existsAfter = set.contains( 783754072 );
        assertTrue( "783754072 should exist before adding here", existedBefore );
        assertFalse( "783754072 should not be reported as added here", added );
        assertTrue( "783754072 should exist", existsAfter );
    }

    @Test
    public void shouldOnlyContainAddedValues_generated_8() throws Exception
    {
        // GIVEN
        PrimitiveLongSet set = new PrimitiveLongHashSet( 7, DEFAULT_HASHING, NO_MONITOR );
        set.add( 375712513 );
        set.remove( 1507941820 );
        set.add( 671750317 );
        set.remove( 1054641019 );
        set.add( 671750317 );
        set.add( 1768202223 );
        set.add( 1768202223 );
        set.add( 1927780165 );
        set.add( 2139399764 );
        set.remove( 1243370828 );
        set.add( 1768202223 );
        set.add( 1335041891 );
        set.remove( 1578984313 );
        set.add( 1227954408 );
        set.remove( 946917826 );
        set.add( 1768202223 );
        set.add( 375712513 );
        set.add( 1668515054 );
        set.add( 401047579 );
        set.add( 33226244 );
        set.add( 126791689 );
        set.add( 401047579 );
        set.add( 1963437853 );
        set.add( 1739617766 );
        set.add( 671750317 );
        set.add( 401047579 );
        set.add( 789094467 );
        set.add( 1291421506 );
        set.add( 1694968582 );
        set.add( 1508353936 );

        // WHEN/THEN
        boolean existedBefore = set.contains( 1739617766 );
        boolean added = set.add( 1739617766 );
        boolean existsAfter = set.contains( 1739617766 );
        assertTrue( "1739617766 should exist before adding here", existedBefore );
        assertFalse( "1739617766 should not be reported as added here", added );
        assertTrue( "1739617766 should exist", existsAfter );
    }

    @Test
    public void shouldContainReallyBigLongValue() throws Exception
    {
        // GIVEN
        PrimitiveLongSet set = new PrimitiveLongHashSet( 10, DEFAULT_HASHING, NO_MONITOR );
        set.add( 7416509207113022571L );

        // WHEN/THEN
        boolean existedBefore = set.contains( 7620037383187366331L );
        boolean added = set.add( 7620037383187366331L );
        boolean existsAfter = set.contains( 7620037383187366331L );
        assertFalse( "7620037383187366331 should not exist before adding here", existedBefore );
        assertTrue( "7620037383187366331 should be reported as added here", added );
        assertTrue( "7620037383187366331 should exist", existsAfter );
    }

    @Test
    public void shouldOnlyContainAddedValues() throws Exception
    {
        // GIVEN
        PrimitiveLongSet set = new PrimitiveLongHashSet( 13, DEFAULT_HASHING,
                new DebugMonitor( new int[] {}, new long[] {5547940863757133161L, 5085293141623130492L, 5547940866926051327L} ) );
        set.add( 52450040186687566L );
        set.add( 52450040186687566L );
        set.add( 5165002753277288833L );
        set.add( 4276883133717080762L );
        set.add( 5547940863757133161L );
        set.add( 8933830774911919116L );
        set.add( 3298254474623565974L );
        set.add( 3366017425691021883L );
        set.add( 8933830774911919116L );
        set.add( 2962608069916354604L );
        set.add( 3366017425691021883L );
        set.remove( 4008464697042048519L );
        set.add( 5547940863757133161L );
        set.add( 52450040186687566L );
        set.add( 4276883133717080762L );
        set.remove( 3298254474623565974L );
        set.remove( 180852386934131061L );
        set.add( 4835176885665539239L );
        set.add( 52450040186687566L );
        set.add( 4591251124405056753L );
        set.add( 5165002753277288833L );
        set.add( 8933830774911919116L );
        set.remove( 3458250832356869483L );
        set.add( 3038543946711308923L );
        set.add( 8743060827282266460L );
        set.add( 5771902951077476377L );
        set.add( 4591251124405056753L );
        set.add( 4835176885665539239L );
        set.remove( 4827343064671369647L );
        set.add( 1533535091190658734L );
        set.remove( 7125666881901305989L );
        set.add( 1533535091190658734L );
        set.add( 52450040186687566L );
        set.remove( 1333521853804287175L );
        set.add( 2962608069916354604L );
        set.add( 5914630622072544054L );
        set.add( 52450040186687566L );
        set.add( 8933830774911919116L );
        set.add( 6198968672674664718L );
        set.add( 6239021001199390909L );
        set.add( 6563452500080365738L );
        set.add( 6128819131542184648L );
        set.add( 5914630622072544054L );
        set.add( 7024933384543504364L );
        set.remove( 3949644814017615281L );
        set.add( 3459376060749741528L );
        set.add( 3201250389951283395L );
        set.add( 4463681497523421181L );
        set.add( 4304197328678536531L );
        set.remove( 4559066538220393098L );
        set.add( 2870119173652414003L );
        set.add( 4048902329274369372L );
        set.add( 3366017425691021883L );
        set.remove( 1092409052848583664L );
        set.add( 7024933384543504364L );
        set.add( 4276883133717080762L );
        set.add( 5914630622072544054L );
        set.add( 4048902329274369372L );
        set.add( 4304197328678536531L );
        set.add( 4151178923662618318L );
        set.remove( 51389524801735953L );
        set.add( 5371788772386487501L );
        set.remove( 8933830774911919116L );
        set.add( 4928410670964886834L );
        set.add( 8306393274966855450L );
        set.add( 2870119173652414003L );
        set.add( 8281622709908651825L );
        set.remove( 9194058056102544672L );
        set.remove( 5547940863757133161L );
        set.add( 9184590238993521817L );
        set.add( 5085293141623130492L );
        set.add( 5633993155928642090L );
        set.remove( 8794875254017117580L );
        set.add( 5894404415376700909L );
        set.add( 4835176885665539239L );
        set.remove( 8743060827282266460L );
        set.remove( 3460096065015553722L );
        set.remove( 3296380689310185627L );
        set.add( 337242488691685550L );
        set.add( 6239021001199390909L );
        set.add( 9104240733803011297L );
        set.add( 807326424150812437L );
        set.add( 3336115330297894183L );
        set.add( 1788796898879121715L );
        set.add( 5756965080438171769L );
        set.remove( 4366313798399763194L );
        set.add( 6198968672674664718L );
        set.add( 486897301084183614L );
        set.add( 2870119173652414003L );
        set.add( 5085293141623130492L );
        set.add( 5771902951077476377L );
        set.remove( 6563452500080365738L );
        set.add( 5347453991851285676L );
        set.add( 7437999035528158926L );
        set.add( 3223908005448803428L );
        set.add( 4300856565210203390L );
        set.remove( 4732570527126410147L );
        set.add( 2180591071166584277L );
        set.add( 5160374384234262648L );
        set.remove( 5165002753277288833L );
        set.add( 4463681497523421181L );
        set.add( 7360196143740041480L );
        set.add( 4928410670964886834L );
        set.add( 807326424150812437L );
        set.remove( 4069279832998820447L );
        set.remove( 337242488691685550L );
        set.add( 3201250389951283395L );
        set.add( 4012293068834101219L );
        set.add( 2333643358471038273L );
        set.add( 1158824602601458449L );
        set.remove( 3906518453155830597L );
        set.add( 7402912598585277900L );
        set.add( 6556025329057634951L );
        set.add( 6684709657047103197L );
        set.remove( 3448774195820272496L );
        set.add( 715736913341007544L );
        set.add( 9104240733803011297L );

        // WHEN/THEN
        boolean existedBefore = set.contains( 1103190229303827372L );
        boolean added = set.add( 1103190229303827372L );
        boolean existsAfter = set.contains( 1103190229303827372L );
        assertFalse( "1103190229303827372 should not exist before adding here", existedBefore );
        assertTrue( "1103190229303827372 should be reported as added here", added );
        assertTrue( "1103190229303827372 should exist", existsAfter );
    }

    private Monitor onlyAllowGrowing( final int growthsToAllow )
    {
        return new Monitor.Adapter()
        {
            private int observedGrowths;

            @Override
            public boolean tableGrew( int fromCapacity, int toCapacity, int currentSize )
            {
                if ( observedGrowths++ == growthsToAllow )
                {
                    fail( "This test should not need to grow table these many times" );
                }
                return true;
            }
        };
    }
}
