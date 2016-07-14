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
package org.neo4j.coreedge.raft.log.segmented;

import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TermsTest
{
    private Terms terms;

    @Test
    public void shouldHaveCorrectInitialValues() throws Exception
    {
        // given
        long prevIndex = 5;
        long prevTerm = 10;
        terms = new Terms( prevIndex, prevTerm );

        // then
        assertTermInRange( -1, prevIndex, ( index ) -> -1L );
        assertEquals( prevTerm, terms.get( prevIndex ) );
        assertTermInRange( prevIndex + 1, prevIndex + 10, ( index ) -> -1L );
    }

    @Test
    public void shouldReturnAppendedTerms() throws Exception
    {
        // given
        terms = new Terms( -1, -1 );
        int count = 10;

        // when
        appendRange( 0, count, ( index ) -> index * 2L );

        // then
        assertTermInRange( 0, count, ( index ) -> index * 2L );
        assertEquals( -1, terms.get( -1 ) );
        assertEquals( -1, terms.get( count ) );
    }

    @Test
    public void shouldReturnAppendedTermsLongerRanges() throws Exception
    {
        terms = new Terms( -1, -1 );
        int count = 10;

        // when
        for ( long term = 0; term < count; term++ )
        {
            appendRange( term * count, (term + 1) * count, term );
        }

        // then
        for ( long term = 0; term < count; term++ )
        {
            assertTermInRange( term * count, (term + 1) * count, term );
        }
    }

    @Test
    public void shouldOnlyAcceptInOrderIndexes() throws Exception
    {
        // given
        long prevIndex = 3;
        long term = 3;
        terms = new Terms( prevIndex, term );

        try
        {
            // when
            terms.append( prevIndex, term );
            fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }

        terms.append( prevIndex + 1, term ); // should work fine
        terms.append( prevIndex + 2, term ); // should work fine
        terms.append( prevIndex + 3, term ); // should work fine

        try
        {
            // when
            terms.append( prevIndex + 5, term );
            fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }

        terms.append( prevIndex + 4, term ); // should work fine
        terms.append( prevIndex + 5, term ); // should work fine
        terms.append( prevIndex + 6, term ); // should work fine
    }

    @Test
    public void shouldOnlyAcceptMonotonicTerms() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        terms.append( prevIndex + 1, term );
        terms.append( prevIndex + 2, term );
        terms.append( prevIndex + 3, term + 1 );
        terms.append( prevIndex + 4, term + 1 );
        terms.append( prevIndex + 5, term + 2 );
        terms.append( prevIndex + 6, term + 2 );

        // when
        try
        {
            terms.append( prevIndex + 7, term + 1 );
            fail();
        }
        catch ( IllegalStateException e )
        {
            // then: expected
        }
    }

    @Test
    public void shouldTruncateInCurrentRange() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, 20, term );
        assertEquals( term, terms.get( 19 ) );

        // when
        long truncateFromIndex = 15;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, truncateFromIndex, ( index ) -> term );
        assertTermInRange( truncateFromIndex, 30, ( index ) -> -1L );
    }

    @Test
    public void shouldTruncateAtExactBoundary() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term );
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 10;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, prevIndex + 10, term );
        assertTermInRange( prevIndex + 10, truncateFromIndex, -1 );
    }

    @Test
    public void shouldTruncateCompleteCurrentRange() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term );
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 ); // to be half-truncated
        appendRange( prevIndex + 20, prevIndex + 30, term + 2 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 15;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, prevIndex + 10, term );
        assertTermInRange( prevIndex + 10, truncateFromIndex, term + 1 );
        assertTermInRange( truncateFromIndex, prevIndex + 30, -1 );
    }

    @Test
    public void shouldTruncateSeveralCompleteRanges() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // to be half-truncated
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 ); // to be truncated
        appendRange( prevIndex + 20, prevIndex + 30, term + 2 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 5;
        terms.truncate( truncateFromIndex );

        // then
        assertTermInRange( prevIndex + 1, truncateFromIndex, term );
        assertTermInRange( truncateFromIndex, prevIndex + 30, -1 );
    }

    @Test
    public void shouldAppendAfterTruncate() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term ); // to be half-truncated
        appendRange( prevIndex + 10, prevIndex + 20, term + 10 ); // to be truncated

        // when
        long truncateFromIndex = prevIndex + 5;
        terms.truncate( truncateFromIndex );
        appendRange( truncateFromIndex, truncateFromIndex + 20, term + 20 );

        // then
        assertTermInRange( prevIndex + 1, truncateFromIndex, term );
        assertTermInRange( truncateFromIndex, truncateFromIndex + 20, term + 20 );
    }

    @Test
    public void shouldAppendAfterSkip() throws Exception
    {
        // given
        long term = 5;
        long prevIndex = 10;
        terms = new Terms( prevIndex, term );

        appendRange( prevIndex + 1, prevIndex + 10, term );
        appendRange( prevIndex + 10, prevIndex + 20, term + 1 );

        // when
        long skipIndex = 30;
        long skipTerm = term + 2;
        terms.skip( skipIndex, skipTerm );

        // then
        assertTermInRange( prevIndex, skipIndex, -1 );
        assertEquals( skipTerm, terms.get( skipIndex ) );

        // when
        appendRange( skipIndex + 1, skipIndex + 20, skipTerm );

        // then
        assertTermInRange( skipIndex + 1, skipIndex + 20, skipTerm );
    }

    private void assertTermInRange( long from, long to, long expectedTerm )
    {
        assertTermInRange( from, to, ( index ) -> expectedTerm );
    }

    private void assertTermInRange( long from, long to, Function<Long,Long> expectedTermFunction )
    {
        for ( long index = from; index < to; index++ )
        {
            assertEquals( (long) expectedTermFunction.apply( index ), terms.get( index ) );
        }
    }

    private void appendRange( long from, long to, long term )
    {
        appendRange( from, to, ( index ) -> term );
    }

    private void appendRange( long from, long to, Function<Long,Long> termFunction )
    {
        for ( long index = from; index < to; index++ )
        {
            terms.append( index, termFunction.apply( index ) );
        }
    }
}
