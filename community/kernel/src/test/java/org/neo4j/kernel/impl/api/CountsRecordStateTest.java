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
package org.neo4j.kernel.impl.api;

import java.util.Arrays;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.store.counts.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.relationshipKey;

public class CountsRecordStateTest
{
    @Test
    public void shouldReportDifferencesBetweenDifferentStates() throws Exception
    {
        // given
        CountsRecordState oracle = new CountsRecordState();
        CountsRecordState victim = new CountsRecordState();
        oracle.incrementNodeCount( 17, 5 );
        victim.incrementNodeCount( 17, 3 );
        oracle.incrementNodeCount( 12, 9 );
        victim.incrementNodeCount( 12, 9 );
        oracle.incrementRelationshipCount( 1, 2, 3, 19 );
        victim.incrementRelationshipCount( 1, 2, 3, 22 );
        oracle.incrementRelationshipCount( 1, 4, 3, 25 );
        victim.incrementRelationshipCount( 1, 4, 3, 25 );

        // when
        List<CountsRecordState.Difference> differences = oracle.verify( victim );

        // then
        assertThat( differences, hasContent(
                new CountsRecordState.Difference( nodeKey( 17 ), 0, 5, 0, 3 ),
                new CountsRecordState.Difference( relationshipKey( 1, 2, 3 ), 0, 19, 0, 22 )
        ) );
    }

    @Test
    public void shouldNotReportAnythingForEqualStates() throws Exception
    {
        // given
        CountsRecordState oracle = new CountsRecordState();
        CountsRecordState victim = new CountsRecordState();
        oracle.incrementNodeCount( 17, 5 );
        victim.incrementNodeCount( 17, 5 );
        oracle.incrementNodeCount( 12, 9 );
        victim.incrementNodeCount( 12, 9 );
        oracle.incrementRelationshipCount( 1, 4, 3, 25 );
        victim.incrementRelationshipCount( 1, 4, 3, 25 );

        // when
        List<CountsRecordState.Difference> differences = oracle.verify( victim );

        // then
        assertTrue( differences.toString(), differences.isEmpty() );
    }

    @SafeVarargs
    private static <T> Matcher<List<T>> hasContent( final T... contents )
    {
        return new TypeSafeMatcher<List<T>>()
        {
            @Override
            protected boolean matchesSafely( List<T> item )
            {
                if ( item.size() != contents.length )
                {
                    return false;
                }
                for ( int i = 0; i < contents.length; i++ )
                {
                    if ( !contents[i].equals( item.get( i ) ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( Arrays.toString( contents ) );
            }
        };
    }
}
