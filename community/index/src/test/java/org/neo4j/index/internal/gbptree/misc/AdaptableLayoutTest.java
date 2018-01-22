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
package org.neo4j.index.internal.gbptree.misc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class AdaptableLayoutTest
{
    @Rule
    public RandomRule random = new RandomRule();

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {"Fixed", new AdaptableLayoutFixed( 8, 0 )},
                {"Dynamic", new AdaptableLayoutDynamic( 8, 0 )}} );
    }

    @Parameterized.Parameter( 0 )
    public String name;

    @Parameterized.Parameter( 1 )
    public AdaptableLayout layout;

    @Test
    public void keySortOrderMustAlignWithSeed() throws Exception
    {
        // given
        int nbrOfSeeds = 10_000;
        List<Long> seeds = new ArrayList<>( nbrOfSeeds );
        List<AdaptableKey> keys = new ArrayList<>( nbrOfSeeds );
        for ( int i = 0; i < nbrOfSeeds; i++ )
        {
            long seed = random.nextLong();
            seeds.add( seed );
            keys.add( layout.key( seed ) );
        }

        // when
        seeds.sort( Long::compareTo );
        keys.sort( layout );

        // then
        for ( int i = 0; i < nbrOfSeeds; i++ )
        {
            long expected = seeds.get( i );
            long actual = keys.get( i ).value;
            assertTrue( String.format( "Sort order differs on position%d, expected=%d, actual=%d", i, expected, actual ),
                    expected == actual );
        }
    }
}
