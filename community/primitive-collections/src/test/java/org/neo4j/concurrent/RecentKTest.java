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
package org.neo4j.concurrent;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class RecentKTest
{
    @Test
    public void shouldEvictOnOverflow() throws Throwable
    {
        // When & Then
        assertThat( appendSequence( 1, 1, 1, 1, 1, 1, 1 ), yieldsSet( 1 ));
        assertThat( appendSequence( 1, 2, 3, 4, 1, 1, 1 ), yieldsSet( 1, 3, 4 ));
        assertThat( appendSequence( 1, 1, 1, 2, 2, 6, 4, 4, 1, 1, 2, 2, 2, 5, 5 ), yieldsSet( 1, 2, 5 ));
    }

    private Matcher<RecentK<Integer>> yieldsSet( final Integer ... expectedItems )
    {
        return new TypeSafeMatcher<RecentK<Integer>>()
        {
            @Override
            protected boolean matchesSafely( RecentK<Integer> recentK )
            {
                assertThat( recentK.recentItems(), containsInAnyOrder( expectedItems ) );
                assertThat( recentK.recentItems().size(), equalTo( expectedItems.length ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValueList( "[", ",", "]", expectedItems );
            }
        };
    }

    private RecentK<Integer> appendSequence( int ... items )
    {
        RecentK<Integer> recentK = new RecentK<>( 3 );
        for ( int item : items )
        {
            recentK.add( item );
        }
        return recentK;
    }
}